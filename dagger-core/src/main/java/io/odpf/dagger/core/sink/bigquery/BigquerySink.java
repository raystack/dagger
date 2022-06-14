package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.bigquery.BigQuerySinkFactory;
import lombok.Getter;
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
    private final ProtoSerializer protoSerializer;
    @Getter
    private final int batchSize;
    @Getter
    private final BigQuerySinkFactory sinkFactory;

    protected BigquerySink(int batchSize, ProtoSerializer protoSerializer, BigQuerySinkFactory sinkFactory) {
        this.batchSize = batchSize;
        this.protoSerializer = protoSerializer;
        this.sinkFactory = sinkFactory;
    }

    @Override
    public SinkWriter<Row, Void, Void> createWriter(InitContext context, List<Void> states) throws IOException {
        sinkFactory.init();
        OdpfSink odpfSink = sinkFactory.create();
        return new BigquerySinkWriter(protoSerializer, odpfSink, batchSize);
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
