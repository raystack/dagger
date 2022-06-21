package io.odpf.dagger.core.sink.bigquery;

import com.google.common.base.Splitter;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import io.odpf.dagger.core.utils.Constants;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.bigquery.BigQuerySinkFactory;
import io.odpf.depot.error.ErrorType;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class BigquerySink implements Sink<Row, Void, Void, Void> {
    private final ProtoSerializer protoSerializer;
    private final Configuration configuration;
    private final BigQuerySinkFactory sinkFactory;

    protected BigquerySink(Configuration configuration, ProtoSerializer protoSerializer, BigQuerySinkFactory sinkFactory) {
        this.configuration = configuration;
        this.protoSerializer = protoSerializer;
        this.sinkFactory = sinkFactory;
    }

    @Override
    public SinkWriter<Row, Void, Void> createWriter(InitContext context, List<Void> states) throws IOException {
        sinkFactory.init();
        OdpfSink odpfSink = sinkFactory.create();
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(context.metricGroup(), configuration);
        int batchSize = configuration.getInteger(
                Constants.SINK_BIGQUERY_BATCH_SIZE,
                Constants.SINK_BIGQUERY_BATCH_SIZE_DEFAULT);
        String errorsForFailing = configuration.getString(
                Constants.SINK_ERROR_TYPES_FOR_FAILURE,
                Constants.SINK_ERROR_TYPES_FOR_FAILURE_DEFAULT);
        Set<ErrorType> errorTypesForFailing = new HashSet<>();
        for (String s : Splitter.on(",").omitEmptyStrings().split(errorsForFailing)) {
            errorTypesForFailing.add(ErrorType.valueOf(s));
        }
        return new BigquerySinkWriter(protoSerializer, odpfSink, batchSize, errorReporter, errorTypesForFailing);
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
