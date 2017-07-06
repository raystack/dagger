package com.gojek.daggers.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.influxdb.InfluxDB;

import java.util.concurrent.TimeUnit;

public class InfluxRowSink extends RichSinkFunction<Row> {
    private InfluxDB influxDB;
    private InfluxDBFactoryWrapper influxDBFactory;
    private String databaseName;
    private String retentionPolicy;

    public InfluxRowSink(InfluxDBFactoryWrapper influxDBFactory){

        this.influxDBFactory = influxDBFactory;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        influxDB = influxDBFactory.connect(parameters.getString("INFLUX_URL", "")
                                            , parameters.getString("INFLUX_USERNAME", "")
                                            , parameters.getString("INFLUX_PASSWORD", "")
        );
        influxDB.enableBatch(parameters.getInteger("INFLUX_BATCH_SIZE", 0)
                             , parameters.getInteger("INFLUX_FLUSH_DURATION_IN_MILLISECONDS", 0)
                             , TimeUnit.MILLISECONDS
        );
        databaseName = parameters.getString("INFLUX_DATABASE", "");
        retentionPolicy = parameters.getString("INFLUX_RETENTION_POLICY", "");
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
        super.close();
    }

    @Override
    public void invoke(Row value) throws Exception {
        influxDB.write(databaseName, retentionPolicy, null);
    }
}
