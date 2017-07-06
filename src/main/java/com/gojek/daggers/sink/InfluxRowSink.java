package com.gojek.daggers.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hdfs.DFSClient;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxRowSink extends RichSinkFunction<Row> {
    private InfluxDB influxDB;
    private InfluxDBFactoryWrapper influxDBFactory;
    private String[] columnNames;
    private Configuration parameters;
    private String databaseName;
    private String retentionPolicy;
    private String measurementName;

    public InfluxRowSink(InfluxDBFactoryWrapper influxDBFactory, String[] columnNames, Configuration parameters){
        this.influxDBFactory = influxDBFactory;
        this.columnNames = columnNames;
        this.parameters = parameters;
        databaseName = parameters.getString("INFLUX_DATABASE", "");
        retentionPolicy = parameters.getString("INFLUX_RETENTION_POLICY", "");
        measurementName = parameters.getString("INFLUX_MEASUREMENT_NAME", "");
    }

    @Override
    public void open(Configuration unusedDepricatedParameters) throws Exception {
        influxDB = influxDBFactory.connect(parameters.getString("INFLUX_URL", "")
                                            , parameters.getString("INFLUX_USERNAME", "")
                                            , parameters.getString("INFLUX_PASSWORD", "")
        );
        influxDB.enableBatch(parameters.getInteger("INFLUX_BATCH_SIZE", 0)
                             , parameters.getInteger("INFLUX_FLUSH_DURATION_IN_MILLISECONDS", 0)
                             , TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
        super.close();
    }

    @Override
    public void invoke(Row row) throws Exception {
        Point.Builder pointBuilder = Point.measurement(measurementName);
        Map<String, Object> fields = new HashMap<>();

        for(int i=0; i < columnNames.length; i++){
            String columnName = columnNames[i];
            if(columnName.equals("window_timestamp")){
                Timestamp field = (Timestamp) row.getField(i);
                pointBuilder.time(field.getTime(), TimeUnit.MILLISECONDS);
            } else if(columnName.startsWith("tag_")){
                pointBuilder.tag(columnName, (String) row.getField(i));
            }
            else fields.put(columnName, row.getField(i));
        }

        influxDB.write(databaseName, retentionPolicy, pointBuilder.fields(fields).build());
    }
}
