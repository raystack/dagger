package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializerHelper;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.bigquery.BigQuerySinkFactory;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class BigquerySink implements Sink<Row, Void, Void, Void> {


    private final ProtoSerializerHelper protoSerializerHelper;
    private final int batchSize;
    private final Configuration configuration;

    BigquerySink(ProtoSerializerHelper protoSerializerHelper, Configuration configuration) {
        this.protoSerializerHelper = protoSerializerHelper;
        this.batchSize = configuration.getInteger("SINK_BIGQUERY_BATCH_SIZE", Constants.DEFAULT_BATCH_SIZE);
        this.configuration = configuration;
    }

    public static BigquerySinkBuilder builder() {
        return new BigquerySinkBuilder();
    }

    @Override
    public SinkWriter<Row, Void, Void> createWriter(InitContext context, List<Void> states) throws IOException {
        BigQuerySinkFactory factory = new BigQuerySinkFactory(configuration.getParam().toMap(), null, null);
        factory.init();
        OdpfSink odpfSink = factory.create();
        return new BigquerySinkWriter(protoSerializerHelper, odpfSink, batchSize);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<Committer<Void>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }
}
