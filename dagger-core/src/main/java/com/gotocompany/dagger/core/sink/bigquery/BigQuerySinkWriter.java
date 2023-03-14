package com.gotocompany.dagger.core.sink.bigquery;

import com.gotocompany.dagger.common.serde.proto.serialization.ProtoSerializer;
import com.gotocompany.dagger.core.exception.BigQueryWriterException;
import com.gotocompany.dagger.core.metrics.reporters.ErrorReporter;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.stream.Collectors;

@Slf4j
public class BigQuerySinkWriter implements SinkWriter<Row, Void, Void> {
    private final ProtoSerializer protoSerializer;
    private final Sink bigquerySink;
    private final int batchSize;
    private final ErrorReporter errorReporter;
    private final Set<ErrorType> errorTypesForFailing;
    private final List<Message> messages = new ArrayList<>();
    private int currentBatchSize;

    public BigQuerySinkWriter(ProtoSerializer protoSerializer, Sink bigquerySink, int batchSize, ErrorReporter errorReporter, Set<ErrorType> errorTypesForFailing) {
        this.protoSerializer = protoSerializer;
        this.bigquerySink = bigquerySink;
        this.batchSize = batchSize;
        this.errorReporter = errorReporter;
        this.errorTypesForFailing = errorTypesForFailing;
    }

    @Override
    public void write(Row element, Context context) throws IOException {
        log.info("adding row to BQ batch : " + element);
        byte[] key = protoSerializer.serializeKey(element);
        byte[] value = protoSerializer.serializeValue(element);
        Message message = new Message(key, value);
        if (currentBatchSize < batchSize) {
            messages.add(message);
            currentBatchSize++;
        }
        if (currentBatchSize >= batchSize) {
            pushToBq();
            messages.clear();
            currentBatchSize = 0;
        }
    }

    private void pushToBq() throws SinkException, BigQueryWriterException {
        log.info("Pushing " + currentBatchSize + " records to bq");
        SinkResponse sinkResponse;
        try {
            sinkResponse = bigquerySink.pushToSink(messages);
        } catch (Exception e) {
            errorReporter.reportFatalException(e);
            throw e;
        }
        if (sinkResponse.hasErrors()) {
            logErrors(sinkResponse, messages);
            checkAndThrow(sinkResponse);
        }
    }

    protected void checkAndThrow(SinkResponse sinkResponse) throws BigQueryWriterException {
        Map<Boolean, List<ErrorInfo>> failedErrorTypes = sinkResponse.getErrors().values().stream().collect(
                Collectors.partitioningBy(errorInfo -> errorTypesForFailing.contains(errorInfo.getErrorType())));
        failedErrorTypes.get(Boolean.FALSE).forEach(errorInfo -> {
            errorReporter.reportNonFatalException(errorInfo.getException());
        });
        failedErrorTypes.get(Boolean.TRUE).forEach(errorInfo -> {
            errorReporter.reportFatalException(errorInfo.getException());
        });
        if (failedErrorTypes.get(Boolean.TRUE).size() > 0) {
            throw new BigQueryWriterException("Error occurred during writing to BigQuery");
        }
    }

    protected void logErrors(SinkResponse sinkResponse, List<Message> sentMessages) {
        log.error("Failed to push " + sinkResponse.getErrors().size() + " records to BigQuerySink");
        sinkResponse.getErrors().forEach((index, errorInfo) -> {
            Message message = sentMessages.get(index.intValue());
            log.error("Failed to pushed message with metadata {}. The exception was {}. The ErrorType was {}",
                    message.getMetadataString(),
                    errorInfo.getException().getMessage(),
                    errorInfo.getErrorType().name());
        });
    }

    /**
     * This will be called before we checkpoint the Writer's state in Streaming execution mode.
     *
     * @param flush – Whether flushing the un-staged data or not
     * @return The data is ready to commit.
     * @throws IOException – if fail to prepare for a commit.
     */
    @Override
    public List<Void> prepareCommit(boolean flush) throws IOException {
        pushToBq();
        messages.clear();
        currentBatchSize = 0;
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        bigquerySink.close();
    }

    @Override
    public List<Void> snapshotState(long checkpointId) {
        // We don't snapshot anything
        return Collections.emptyList();
    }
}
