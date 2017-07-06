package com.gojek.daggers.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.influxdb.InfluxDB;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class InfluxRowSinkTest {

    Configuration parameters;

    @Mock
    InfluxDBFactoryWrapper influxDBFactory;

    @Mock
    InfluxDB influxDb;

    @Before
    public void setUp(){
        parameters = new Configuration();
        parameters.setString("INFLUX_URL", "http://localhost:1111");
        parameters.setString("INFLUX_USERNAME", "usr");
        parameters.setString("INFLUX_PASSWORD", "pwd");
        parameters.setInteger("INFLUX_BATCH_SIZE", 100);
        parameters.setInteger("INFLUX_FLUSH_DURATION_IN_MILLISECONDS", 1000);
        parameters.setString("INFLUX_DATABASE", "dtest");
        parameters.setString("INFLUX_RETENTION_POLICY", "two_day_policy");
        when(influxDBFactory.connect(any(), any(), any())).thenReturn(influxDb);
    }

    @Test
    public void shouldCallInfluxDbFactoryOnOpen() throws Exception {

        new InfluxRowSink(influxDBFactory).open(parameters);

        verify(influxDBFactory).connect("http://localhost:1111", "usr", "pwd" );
    }

    @Test
    public void shouldCallBatchModeOnInfluxWhenBatchSettingsExist() throws Exception{

        new InfluxRowSink(influxDBFactory).open(parameters);

        verify(influxDb).enableBatch(100, 1000, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldCloseInfluxDBWhenCloseCalled() throws Exception {
        InfluxRowSink influxRowSink = new InfluxRowSink(influxDBFactory);
        influxRowSink.open(parameters);

        influxRowSink.close();

        verify(influxDb).close();
    }

    @Test
    public void shouldWriteToConfiguredInfluxDatabase() throws Exception {
        InfluxRowSink influxRowSink = new InfluxRowSink(influxDBFactory);
        influxRowSink.open(parameters);

        influxRowSink.invoke( new Row(0));

        verify(influxDb).write(eq("dtest"), eq("two_day_policy"), any());
    }

}
