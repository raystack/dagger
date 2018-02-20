package com.gojek.daggers.sink;

import com.google.common.base.Strings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class InfluxRowSink extends RichSinkFunction<Row> implements CheckpointedFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxRowSink.class.getName());

  private InfluxDB influxDB;
  private InfluxDBFactoryWrapper influxDBFactory;
  private String[] columnNames;
  private Configuration parameters;
  private String databaseName;
  private String retentionPolicy;
  private String measurementName;
  private Exception influxError;

  public InfluxRowSink(InfluxDBFactoryWrapper influxDBFactory, String[] columnNames, Configuration parameters) {
    this.influxDBFactory = influxDBFactory;
    this.columnNames = columnNames;
    this.parameters = parameters;
    databaseName = parameters.getString("INFLUX_DATABASE", "");
    retentionPolicy = parameters.getString("INFLUX_RETENTION_POLICY", "");
    measurementName = parameters.getString("INFLUX_MEASUREMENT_NAME", "");
    influxError = null;
  }

  @Override
  public void open(Configuration unusedDepricatedParameters) throws Exception {
    influxDB = influxDBFactory.connect(parameters.getString("INFLUX_URL", ""),
        parameters.getString("INFLUX_USERNAME", ""),
        parameters.getString("INFLUX_PASSWORD", "")
    );

    BiConsumer<Iterable<Point>, Throwable> exceptionHandler = (points, throwable) -> {
        influxError = new Exception(throwable);
        points.forEach(point -> LOGGER.error("Error writing to influx {}", point.toString()));
    };
    influxDB.enableBatch(parameters.getInteger("INFLUX_BATCH_SIZE", 0),
        parameters.getInteger("INFLUX_FLUSH_DURATION_IN_MILLISECONDS", 0),
        TimeUnit.MILLISECONDS,  Executors.defaultThreadFactory(), exceptionHandler
    );
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
      if (influxError != null) {
          throw influxError;
      }
      influxDB.write(databaseName, retentionPolicy, pointBuilder.fields(fields).build());
  }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (influxError != null) {
            throw influxError;
        }
        influxDB.flush();
        if (influxError != null) {
            throw influxError;
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // do nothing
    }
}
