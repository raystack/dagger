package org.raystack.dagger.core.sink.bigquery;

import com.google.common.base.Splitter;
import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.serde.proto.serialization.ProtoSerializer;
import org.raystack.dagger.core.metrics.reporters.ErrorReporter;
import org.raystack.dagger.core.metrics.reporters.ErrorReporterFactory;
import org.raystack.dagger.core.metrics.reporters.statsd.DaggerStatsDReporter;
import org.raystack.dagger.core.utils.Constants;
import org.raystack.depot.bigquery.BigQuerySinkFactory;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.error.ErrorType;
import org.aeonbits.owner.ConfigFactory;
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

public class BigQuerySink implements Sink<Row, Void, Void, Void> {
    private final ProtoSerializer protoSerializer;
    private final Configuration configuration;
    private final DaggerStatsDReporter daggerStatsDReporter;
    private transient BigQuerySinkFactory sinkFactory;

    protected BigQuerySink(Configuration configuration, ProtoSerializer protoSerializer, DaggerStatsDReporter daggerStatsDReporter) {
        this(configuration, protoSerializer, null, daggerStatsDReporter);
    }

    /**
     * Constructor for testing.
     */
    protected BigQuerySink(Configuration configuration, ProtoSerializer protoSerializer, BigQuerySinkFactory sinkFactory, DaggerStatsDReporter daggerStatsDReporter) {
        this.configuration = configuration;
        this.protoSerializer = protoSerializer;
        this.sinkFactory = sinkFactory;
        this.daggerStatsDReporter = daggerStatsDReporter;
    }

    @Override
    public SinkWriter<Row, Void, Void> createWriter(InitContext context, List<Void> states) {
        ErrorReporter errorReporter = ErrorReporterFactory.getErrorReporter(context.metricGroup(), configuration);
        if (sinkFactory == null) {
            BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configuration.getParam().toMap());
            sinkFactory = new BigQuerySinkFactory(sinkConfig, daggerStatsDReporter.buildStatsDReporter());
            try {
                sinkFactory.init();
            } catch (Exception e) {
                errorReporter.reportFatalException(e);
                throw e;
            }
        }
        org.raystack.depot.Sink sink = sinkFactory.create();
        int batchSize = configuration.getInteger(
                Constants.SINK_BIGQUERY_BATCH_SIZE,
                Constants.SINK_BIGQUERY_BATCH_SIZE_DEFAULT);
        String errorsForFailing = configuration.getString(
                Constants.SINK_ERROR_TYPES_FOR_FAILURE,
                Constants.SINK_ERROR_TYPES_FOR_FAILURE_DEFAULT);
        Set<ErrorType> errorTypesForFailing = new HashSet<>();
        for (String s : Splitter.on(",").omitEmptyStrings().split(errorsForFailing)) {
            errorTypesForFailing.add(ErrorType.valueOf(s.trim()));
        }
        return new BigQuerySinkWriter(protoSerializer, sink, batchSize, errorReporter, errorTypesForFailing);
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
