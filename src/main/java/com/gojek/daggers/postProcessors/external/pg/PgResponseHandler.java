package com.gojek.daggers.postProcessors.external.pg;

import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.protoHandler.ProtoHandler;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.google.protobuf.Descriptors;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.sqlclient.RowSet;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.INVALID_CONFIGURATION;
import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.SUCCESS_RESPONSE_TIME;
import static com.gojek.daggers.protoHandler.RowFactory.createRow;
import static java.time.Duration.between;
import static java.util.Collections.singleton;

public class PgResponseHandler implements Handler<AsyncResult<RowSet<io.vertx.sqlclient.Row>>> {
    private final PgSourceConfig pgSourceConfig;
    private final MeterStatsManager meterStatsManager;
    private final RowManager rowManager;
    private final ColumnNameManager columnNameManager;
    private final Descriptors.Descriptor outputDescriptor;
    private final ResultFuture<Row> resultFuture;
    private final ErrorReporter errorReporter;
    private Instant startTime;

    public PgResponseHandler(PgSourceConfig pgSourceConfig, MeterStatsManager meterStatsManager, RowManager rowManager, ColumnNameManager columnNameManager, Descriptors.Descriptor outputDescriptor, ResultFuture<Row> resultFuture, ErrorReporter errorReporter) {

        this.pgSourceConfig = pgSourceConfig;
        this.meterStatsManager = meterStatsManager;
        this.rowManager = rowManager;
        this.columnNameManager = columnNameManager;
        this.outputDescriptor = outputDescriptor;
        this.resultFuture = resultFuture;
        this.errorReporter = errorReporter;
    }

    public void startTimer() {
        startTime = Instant.now();
    }

    @Override
    public void handle(AsyncResult event) {
        if (event.succeeded()) {
            try {
                List<String> pgOutputColumnNames = pgSourceConfig.getOutputColumns();
                RowSet<io.vertx.sqlclient.Row> rows = (RowSet<io.vertx.sqlclient.Row>) event.result();
                pgOutputColumnNames.forEach(outputColumnName -> {
                    for (io.vertx.sqlclient.Row row : rows) {
                        int outputColumnIndex = columnNameManager.getOutputIndex(outputColumnName);
                        Object outputColumnValue = row.getValue(pgSourceConfig.getMappedQueryParam(outputColumnName));
                        setField(outputColumnIndex, outputColumnValue, outputColumnName);
                    }
                });
            } finally {
                meterStatsManager.updateHistogram(SUCCESS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
                resultFuture.complete(singleton(rowManager.getAll()));
            }

        }
    }

    private void setField(int index, Object value, String name) {
        if (!pgSourceConfig.hasType()) {
            rowManager.setInOutput(index, value);
            return;
        }
        Descriptors.FieldDescriptor fieldDescriptor = outputDescriptor.findFieldByName(name);
        if (fieldDescriptor == null) {
            Exception illegalArgumentException = new IllegalArgumentException("Field Descriptor not found for field: " + name);
            reportAndThrowError(illegalArgumentException);
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            return;
        }
        if (value instanceof Map) {
            rowManager.setInOutput(index, createRow((Map<String, Object>) value, fieldDescriptor.getMessageType()));
        } else {
            ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
            rowManager.setInOutput(index, protoHandler.transform(value));
        }
    }

    private void reportAndThrowError(Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }

}
