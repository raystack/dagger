package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializerHelper;
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
    private final ProtoSerializerHelper protoSerializerHelper;
    private final OdpfSink bigquerySink;
    private final int batchSize;
    private int currentBatchSize;
    private final List<OdpfMessage> messages = new ArrayList<>();

    public BigquerySinkWriter(ProtoSerializerHelper protoSerializerHelper, OdpfSink bigquerySink, int batchSize) {
        this.protoSerializerHelper = protoSerializerHelper;
        this.bigquerySink = bigquerySink;
        this.batchSize = batchSize;
    }

    @Override
    public void write(Row element, Context context) throws IOException {
        log.info("row to BQ: " + element);
        byte[] key = protoSerializerHelper.serializeKey(element);
        byte[] value = protoSerializerHelper.serializeValue(element);
        OdpfMessage message = new OdpfMessage(key, value);
        if (currentBatchSize < batchSize) {
            messages.add(message);
            currentBatchSize++;
        }
        if (currentBatchSize >= batchSize) {
            log.info("Pushing " + currentBatchSize + " records to bq");
            OdpfSinkResponse odpfSinkResponse = bigquerySink.pushToSink(messages);
            if (odpfSinkResponse.hasErrors()) {
                log.error("Failed record " + odpfSinkResponse.getErrors().size());
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
