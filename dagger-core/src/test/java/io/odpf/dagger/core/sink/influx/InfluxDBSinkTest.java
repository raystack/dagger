package io.odpf.dagger.core.sink.influx;

import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.utils.Constants;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static io.odpf.dagger.core.utils.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class InfluxDBSinkTest {

    private static final int SINK_INFLUX_BATCH_SIZE = 100;
    private static final int INFLUX_FLUSH_DURATION = 1000;

    @Mock
    private Configuration configuration;

    @Mock
    private InitContext context;

    @Mock
    private InfluxDBFactoryWrapper influxDBFactory;

    @Mock
    private InfluxDB influxDb;

    @Mock
    private Counter counter;

    @Mock
    private SinkWriterMetricGroup metricGroup;

    private ErrorHandler errorHandler = new ErrorHandler();

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        when(configuration.getString(SINK_INFLUX_URL_KEY, SINK_INFLUX_URL_DEFAULT)).thenReturn("http://localhost:1111");
        when(configuration.getString(SINK_INFLUX_USERNAME_KEY, SINK_INFLUX_USERNAME_DEFAULT)).thenReturn("usr");
        when(configuration.getString(SINK_INFLUX_PASSWORD_KEY, SINK_INFLUX_PASSWORD_DEFAULT)).thenReturn("pwd");

        when(configuration.getInteger(SINK_INFLUX_BATCH_SIZE_KEY, SINK_INFLUX_BATCH_SIZE_DEFAULT)).thenReturn(100);
        when(configuration.getInteger(SINK_INFLUX_FLUSH_DURATION_MS_KEY, SINK_INFLUX_FLUSH_DURATION_MS_DEFAULT)).thenReturn(1000);

        when(influxDBFactory.connect(any(), any(), any())).thenReturn(influxDb);
        when(context.metricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.SINK_INFLUX_LATE_RECORDS_DROPPED_KEY)).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY,
                InfluxDBException.class.getName())).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
    }


    @Test
    public void shouldCallInfluxDbFactoryWhileCreatingWriter() throws Exception {
        InfluxDBSink influxDBSink = new InfluxDBSink(influxDBFactory, configuration, new String[]{}, errorHandler);
        List<Void> state = new ArrayList<>();
        influxDBSink.createWriter(context, state);

        verify(influxDBFactory).connect("http://localhost:1111", "usr", "pwd");
    }

    @Test
    public void shouldCreateInfluxWriter() throws IOException {
        InfluxDBSink influxDBSink = new InfluxDBSink(influxDBFactory, configuration, new String[]{}, errorHandler);
        List<Void> state = new ArrayList<>();
        SinkWriter<Row, Void, Void> writer = influxDBSink.createWriter(context, state);

        assertEquals(writer.getClass(), InfluxDBWriter.class);
    }

    @Test
    public void shouldCallBatchModeOnInfluxWhenBatchSettingsExist() throws Exception {
        InfluxDBSink influxDBSink = new InfluxDBSink(influxDBFactory, configuration, new String[]{}, errorHandler);
        List<Void> state = new ArrayList<>();
        influxDBSink.createWriter(context, state);
        verify(influxDb).enableBatch(eq(SINK_INFLUX_BATCH_SIZE), eq(INFLUX_FLUSH_DURATION), eq(TimeUnit.MILLISECONDS), any(ThreadFactory.class), any(BiConsumer.class));
    }
}
