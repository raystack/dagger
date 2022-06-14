package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.message.OdpfMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BigquerySinkWriter implements SinkWriter<Row, Void, Void> {
    private final ProtoSerializer protoSerializer;
    private final OdpfSink bigquerySink;
    private final int batchSize;
    private int currentBatchSize;
    private final List<OdpfMessage> messages = new ArrayList<>();

    public BigquerySinkWriter(ProtoSerializer protoSerializer, OdpfSink bigquerySink, int batchSize) {
        this.protoSerializer = protoSerializer;
        this.bigquerySink = bigquerySink;
        this.batchSize = batchSize;
    }

    @Override
    public void write(Row element, Context context) throws IOException {
        log.info("adding row to BQ batch : " + element);
        byte[] key = protoSerializer.serializeKey(element);
        byte[] value = protoSerializer.serializeValue(element);
        OdpfMessage message = new OdpfMessage(key, value);
        if (currentBatchSize < batchSize) {
            messages.add(message);
            currentBatchSize++;
        }
        if (currentBatchSize >= batchSize) {
            log.info("Pushing " + currentBatchSize + " records to bq");
            OdpfSinkResponse odpfSinkResponse = bigquerySink.pushToSink(messages);
            if (odpfSinkResponse.hasErrors()) {
                log.error("Failed to push " + odpfSinkResponse.getErrors().size() + " records to BigquerySink");
                throw new IOException("Failed to push " + odpfSinkResponse.getErrors().size() + " records to BigquerySink");
            }
            messages.clear();
            currentBatchSize = 0;
        }
    }

    @Override
    public List<Void> prepareCommit(boolean flush) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws Exception {
        bigquerySink.close();
    }
}
