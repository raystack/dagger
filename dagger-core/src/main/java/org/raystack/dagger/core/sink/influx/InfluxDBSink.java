package org.raystack.dagger.core.sink.influx;

import org.raystack.dagger.core.metrics.reporters.ErrorReporter;
import org.raystack.dagger.core.metrics.reporters.ErrorReporterFactory;
import org.raystack.dagger.core.utils.Constants;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import org.raystack.dagger.common.configuration.Configuration;
import org.influxdb.InfluxDB;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InfluxDBSink implements Sink<Row, Void, Void, Void> {
    private InfluxDBFactoryWrapper influxDBFactory;
    private Configuration configuration;
    private String[] columnNames;
    private ErrorHandler errorHandler;
    private ErrorReporter errorReporter;

    public InfluxDBSink(InfluxDBFactoryWrapper influxDBFactory, Configuration configuration, String[] columnNames, ErrorHandler errorHandler) {
        this.influxDBFactory = influxDBFactory;
        this.configuration = configuration;
        this.columnNames = columnNames;
        this.errorHandler = errorHandler;
    }

    @Override
    public SinkWriter<Row, Void, Void> createWriter(InitContext context, List<Void> states) throws IOException {
        InfluxDB influxDB = influxDBFactory.connect(configuration.getString(Constants.SINK_INFLUX_URL_KEY, Constants.SINK_INFLUX_URL_DEFAULT),
                configuration.getString(Constants.SINK_INFLUX_USERNAME_KEY, Constants.SINK_INFLUX_USERNAME_DEFAULT),
                configuration.getString(Constants.SINK_INFLUX_PASSWORD_KEY, Constants.SINK_INFLUX_PASSWORD_DEFAULT));
        errorHandler.init(context);
        influxDB.enableBatch(configuration.getInteger(Constants.SINK_INFLUX_BATCH_SIZE_KEY, Constants.SINK_INFLUX_BATCH_SIZE_DEFAULT),
                configuration.getInteger(Constants.SINK_INFLUX_FLUSH_DURATION_MS_KEY, Constants.SINK_INFLUX_FLUSH_DURATION_MS_DEFAULT),
                TimeUnit.MILLISECONDS, Executors.defaultThreadFactory(), errorHandler.getExceptionHandler());
        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(context.metricGroup(), configuration);
        }

        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDB, columnNames, errorHandler, errorReporter);
        return influxDBWriter;
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
