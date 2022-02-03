package io.odpf.dagger.core.sink.influx;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.types.Row;

import com.google.common.base.Strings;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.odpf.dagger.core.utils.Constants.*;

public class InfluxDBWriter implements SinkWriter<Row, Void, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBWriter.class.getName());
    private final String databaseName;
    private final String retentionPolicy;
    private final String measurementName;
    private InfluxDB influxDB;
    private String[] columnNames;
    private ErrorHandler errorHandler;
    private ErrorReporter errorReporter;

    public InfluxDBWriter(Configuration configuration, InfluxDB influxDB, String[] columnNames, ErrorHandler errorHandler, ErrorReporter errorReporter) {
        databaseName = configuration.getString(SINK_INFLUX_DB_NAME_KEY, SINK_INFLUX_DB_NAME_DEFAULT);
        retentionPolicy = configuration.getString(SINK_INFLUX_RETENTION_POLICY_KEY, SINK_INFLUX_RETENTION_POLICY_DEFAULT);
        measurementName = configuration.getString(SINK_INFLUX_MEASUREMENT_NAME_KEY, SINK_INFLUX_MEASUREMENT_NAME_DEFAULT);
        this.influxDB = influxDB;
        this.columnNames = columnNames;
        this.errorHandler = errorHandler;
        this.errorReporter = errorReporter;
    }

    @Override
    public void write(Row row, Context context) throws IOException, InterruptedException {
        LOGGER.info("row to influx: " + row);

        Builder pointBuilder = Point.measurement(measurementName);
        Map<String, Object> fields = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            if (columnName.equals("window_timestamp")) {
                LocalDateTime timeField = (LocalDateTime) row.getField(i);
                ZonedDateTime zonedDateTime = timeField.atZone(ZoneOffset.UTC);
                pointBuilder.time(zonedDateTime.toInstant().toEpochMilli(), TimeUnit.MILLISECONDS);
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
    public List<Void> prepareCommit(boolean flush) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
    }


    private void addErrorMetricsAndThrow() throws IOException {
        if (errorHandler.getError().isPresent() && errorHandler.getError().get().hasException()) {
            IOException currentException = errorHandler.getError().get().getCurrentException();
            errorReporter.reportFatalException(currentException);
            throw currentException;
        }
    }

    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        addErrorMetricsAndThrow();
        try {
            influxDB.flush();
        } catch (Exception exception) {
            errorReporter.reportFatalException(exception);
            throw exception;
        }
        addErrorMetricsAndThrow();
        return Collections.emptyList();
    }
}
