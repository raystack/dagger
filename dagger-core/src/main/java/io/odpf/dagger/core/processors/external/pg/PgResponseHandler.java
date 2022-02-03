package io.odpf.dagger.core.processors.external.pg;

import io.odpf.dagger.core.exception.HttpFailureException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.PostResponseTelemetry;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.common.serde.proto.protohandler.ProtoHandler;
import io.odpf.dagger.common.serde.proto.protohandler.ProtoHandlerFactory;
import com.google.protobuf.Descriptors;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.sqlclient.RowSet;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.common.serde.proto.protohandler.RowFactory.createRow;

/**
 * The Postgre response handler.
 */
public class PgResponseHandler implements Handler<AsyncResult<RowSet<io.vertx.sqlclient.Row>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PgResponseHandler.class.getName());
    private final PgSourceConfig pgSourceConfig;
    private final MeterStatsManager meterStatsManager;
    private final RowManager rowManager;
    private final ColumnNameManager columnNameManager;
    private final Descriptors.Descriptor outputDescriptor;
    private final ResultFuture<Row> resultFuture;
    private final ErrorReporter errorReporter;
    private PostResponseTelemetry postResponseTelemetry;
    private Instant startTime;

    /**
     * Instantiates a new Postgre response handler.
     *
     * @param pgSourceConfig        the pg source config
     * @param meterStatsManager     the meter stats manager
     * @param rowManager            the row manager
     * @param columnNameManager     the column name manager
     * @param outputDescriptor      the output descriptor
     * @param resultFuture          the result future
     * @param errorReporter         the error reporter
     * @param postResponseTelemetry the post response telemetry
     */
    public PgResponseHandler(PgSourceConfig pgSourceConfig, MeterStatsManager meterStatsManager, RowManager rowManager, ColumnNameManager columnNameManager, Descriptors.Descriptor outputDescriptor, ResultFuture<Row> resultFuture, ErrorReporter errorReporter, PostResponseTelemetry postResponseTelemetry) {

        this.pgSourceConfig = pgSourceConfig;
        this.meterStatsManager = meterStatsManager;
        this.rowManager = rowManager;
        this.columnNameManager = columnNameManager;
        this.outputDescriptor = outputDescriptor;
        this.resultFuture = resultFuture;
        this.errorReporter = errorReporter;
        this.postResponseTelemetry = postResponseTelemetry;
    }

    /**
     * Start timer.
     */
    public void startTimer() {
        startTime = Instant.now();
    }

    @Override
    public void handle(AsyncResult<RowSet<io.vertx.sqlclient.Row>> event) {
        if (event.succeeded()) {
            successHandler(event.result());
        } else {
            failureHandler(event.cause());
        }
    }

    private void successHandler(RowSet<io.vertx.sqlclient.Row> resultRowSet) {
        if (resultRowSet.size() > 1) {
            meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            Exception illegalArgumentException = new IllegalArgumentException("Invalid query resulting in more than one rows. ");
            if (pgSourceConfig.isFailOnErrors()) {
                reportAndThrowError(illegalArgumentException);
            } else {
                errorReporter.reportNonFatalException(illegalArgumentException);
                resultFuture.complete(Collections.singleton(rowManager.getAll()));
            }
            return;
        }
        List<String> pgOutputColumnNames = pgSourceConfig.getOutputColumns();
        pgOutputColumnNames.forEach(outputColumnName -> {
            for (io.vertx.sqlclient.Row row : resultRowSet) {
                int outputColumnIndex = columnNameManager.getOutputIndex(outputColumnName);
                String mappedQueryParam = pgSourceConfig.getMappedQueryParam(outputColumnName);
                if (row.getColumnIndex(mappedQueryParam) == -1) {
                    Exception illegalArgumentException = new IllegalArgumentException("Invalid field " + mappedQueryParam + " is not present in the SQL. ");
                    reportAndThrowError(illegalArgumentException);
                    meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
                    return;
                } else {
                    setField(outputColumnIndex, row.getValue(mappedQueryParam), outputColumnName);
                }
            }
        });
        postResponseTelemetry.sendSuccessTelemetry(meterStatsManager, startTime);
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    private void failureHandler(Throwable e) {
        postResponseTelemetry.sendFailureTelemetry(meterStatsManager, startTime);
        LOGGER.error(e.getMessage());
        Exception httpFailureException = new HttpFailureException("PgResponseHandler : Failed with error. " + e.getMessage());
        if (pgSourceConfig.isFailOnErrors()) {
            reportAndThrowError(httpFailureException);
        } else {
            errorReporter.reportNonFatalException(httpFailureException);
        }
        if (e instanceof ResponseException) {
            postResponseTelemetry.validateResponseCode(meterStatsManager, ((ResponseException) e).getResponse().getStatusLine().getStatusCode());
        } else {
            meterStatsManager.markEvent(ExternalSourceAspects.OTHER_ERRORS);
            System.err.printf("PGResponseHandler some other errors :  %s \n", e.getMessage());
        }
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    private void setField(int index, Object value, String name) {
        if (!pgSourceConfig.isRetainResponseType() || pgSourceConfig.hasType()) {
            Descriptors.FieldDescriptor fieldDescriptor = outputDescriptor.findFieldByName(name);
            if (fieldDescriptor == null) {
                Exception illegalArgumentException = new IllegalArgumentException("Field Descriptor not found for field: " + name);
                reportAndThrowError(illegalArgumentException);
                meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
                return;
            }
            if (value instanceof Map) {
                rowManager.setInOutput(index, createRow((Map<String, Object>) value, fieldDescriptor.getMessageType()));
            } else {
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
                rowManager.setInOutput(index, protoHandler.transformFromPostProcessor(value));
            }
        } else {
            rowManager.setInOutput(index, value);
        }
    }

    private void reportAndThrowError(Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }
}
