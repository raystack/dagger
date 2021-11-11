package io.odpf.dagger.core.sink.log;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Optional;

/**
 * The Log sink.
 */
public class LogSink implements Sink<Row, Void, Void, Void> {
    private final String[] columnNames;

    /**
     * Instantiates a new Log sink.
     *
     * @param columnNames the column names
     */
    public LogSink(String[] columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public SinkWriter createWriter(InitContext context, List states) {
        return new LogSinkWriter(columnNames);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<Committer<Void>> createCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() {
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
