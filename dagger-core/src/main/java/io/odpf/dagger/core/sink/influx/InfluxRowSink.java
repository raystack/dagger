package io.odpf.dagger.core.sink.influx;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import com.google.common.base.Strings;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.odpf.dagger.core.utils.Constants.*;

/**
 * The Influx row sink.
 */
public class InfluxRowSink extends RichSinkFunction<Row> implements CheckpointedFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxRowSink.class.getName());

    private InfluxDB influxDB;
    private InfluxDBFactoryWrapper influxDBFactory;
    private String[] columnNames;
    private Configuration configurations;
    private String databaseName;
    private String retentionPolicy;
    private String measurementName;
    private ErrorHandler errorHandler;
    private ErrorReporter errorReporter;

    /**
     * Instantiates a new Influx row sink.
     *
     * @param influxDBFactory   the influx db factory
     * @param columnNames       the column names
     * @param configuration the userConfiguration
     * @param errorHandler      the error handler
     */
    public InfluxRowSink(InfluxDBFactoryWrapper influxDBFactory, String[] columnNames, Configuration configuration, ErrorHandler errorHandler) {
        this.influxDBFactory = influxDBFactory;
        this.columnNames = columnNames;
        this.configurations = configuration;
        this.errorHandler = errorHandler;
        databaseName = configuration.getString(SINK_INFLUX_DB_NAME_KEY, SINK_INFLUX_DB_NAME_DEFAULT);
        retentionPolicy = configuration.getString(SINK_INFLUX_RETENTION_POLICY_KEY, SINK_INFLUX_RETENTION_POLICY_DEFAULT);
        measurementName = configuration.getString(SINK_INFLUX_MEASUREMENT_NAME_KEY, SINK_INFLUX_MEASUREMENT_NAME_DEFAULT);
    }

    /**
     * Instantiates a new Influx row sink with specified error reporter.
     *
     * @param influxDBFactory   the influx db factory
     * @param columnNames       the column names
     * @param configuration the userConfigurations
     * @param errorHandler      the error handler
     * @param errorReporter     the error reporter
     */
    public InfluxRowSink(InfluxDBFactoryWrapper influxDBFactory, String[] columnNames, Configuration configuration, ErrorHandler errorHandler, ErrorReporter errorReporter) {
        this.influxDBFactory = influxDBFactory;
        this.columnNames = columnNames;
        this.configurations = configuration;
        this.errorHandler = errorHandler;
        this.errorReporter = errorReporter;
        databaseName = configuration.getString(SINK_INFLUX_DB_NAME_KEY, SINK_INFLUX_DB_NAME_DEFAULT);
        retentionPolicy = configuration.getString(SINK_INFLUX_RETENTION_POLICY_KEY, SINK_INFLUX_RETENTION_POLICY_DEFAULT);
        measurementName = configuration.getString(SINK_INFLUX_MEASUREMENT_NAME_KEY, SINK_INFLUX_MEASUREMENT_NAME_DEFAULT);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration internalFlinkConfig) throws Exception {
        errorHandler.init(getRuntimeContext());
        influxDB = influxDBFactory.connect(configurations.getString(SINK_INFLUX_URL_KEY, SINK_INFLUX_URL_DEFAULT),
                configurations.getString(SINK_INFLUX_USERNAME_KEY, SINK_INFLUX_USERNAME_DEFAULT),
                configurations.getString(SINK_INFLUX_PASSWORD_KEY, SINK_INFLUX_PASSWORD_DEFAULT)
        );

        influxDB.enableBatch(configurations.getInteger(SINK_INFLUX_BATCH_SIZE_KEY, SINK_INFLUX_BATCH_SIZE_DEFAULT),
                configurations.getInteger(SINK_INFLUX_FLUSH_DURATION_MS_KEY, SINK_INFLUX_FLUSH_DURATION_MS_DEFAULT),
                TimeUnit.MILLISECONDS, Executors.defaultThreadFactory(), errorHandler.getExceptionHandler()
        );
        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext(), configurations);
        }
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
        super.close();
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        LOGGER.info("row to influx: " + row);
        Point.Builder pointBuilder = Point.measurement(measurementName);
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            if (columnName.equals("window_timestamp")) {
                Timestamp field = (Timestamp) row.getField(i);
                pointBuilder.time(field.getTime(), TimeUnit.MILLISECONDS);
            } else if (columnName.startsWith("tag_")) {
                pointBuilder.tag(columnName, String.valueOf(row.getField(i)));
            } else if (columnName.startsWith("label_")) {
                pointBuilder.tag(columnName.substring("label_".length()), ((String) row.getField(i)));
            } else {
                if (!(Strings.isNullOrEmpty(columnName) || row.getField(i) == null)) {
                    fields.put(columnName, row.getField(i));
                }
            }
        }
        addErrorMetricsAndThrow();
        try {
            influxDB.write(databaseName, retentionPolicy, pointBuilder.fields(fields).build());
        } catch (Exception exception) {
            errorReporter.reportFatalException(exception);
            throw exception;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        addErrorMetricsAndThrow();
        try {
            influxDB.flush();
        } catch (Exception exception) {
            errorReporter.reportFatalException(exception);
            throw exception;
        }
        addErrorMetricsAndThrow();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // do nothing
    }

    private void addErrorMetricsAndThrow() throws Exception {
        if (errorHandler.getError().isPresent() && errorHandler.getError().get().hasException()) {
            Exception currentException = errorHandler.getError().get().getCurrentException();
            errorReporter.reportFatalException(currentException);
            throw currentException;
        }
    }
}
