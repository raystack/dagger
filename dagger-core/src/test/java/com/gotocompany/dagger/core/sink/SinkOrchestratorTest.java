package com.gotocompany.dagger.core.sink;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.core.metrics.reporters.statsd.DaggerStatsDReporter;
import com.gotocompany.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import com.gotocompany.dagger.core.sink.bigquery.BigQuerySink;
import com.gotocompany.dagger.core.sink.influx.InfluxDBSink;
import com.gotocompany.dagger.core.sink.log.LogSink;
import com.gotocompany.dagger.core.utils.Constants;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.*;

import static com.gotocompany.dagger.common.core.Constants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkOrchestratorTest {

    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private SinkOrchestrator sinkOrchestrator;
    @Mock
    private MetricsTelemetryExporter telemetryExporter;
    @Mock
    private DaggerStatsDReporter daggerStatsDReporter;

    @Before
    public void setup() {
        initMocks(this);
        configuration = mock(Configuration.class, withSettings().serializable());
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);

        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        sinkOrchestrator = new SinkOrchestrator(telemetryExporter);
    }

    @Test
    public void shouldGiveInfluxSinkWhenConfiguredToUseInflux() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("influx");
        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator, daggerStatsDReporter);

        assertThat(sinkFunction, instanceOf(InfluxDBSink.class));
    }

    @Test
    public void shouldGiveLogSinkWhenConfiguredToUseLog() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("log");
        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator, daggerStatsDReporter);

        assertThat(sinkFunction, instanceOf(LogSink.class));
    }

    @Test
    public void shouldGiveInfluxWhenConfiguredToUseNothing() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("");
        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator, daggerStatsDReporter);

        assertThat(sinkFunction, instanceOf(InfluxDBSink.class));
    }

    @Test
    public void shouldSetKafkaProducerConfigurations() throws Exception {
        when(configuration.getString(eq(Constants.SINK_KAFKA_BROKERS_KEY), anyString())).thenReturn("10.200.216.87:6668");
        when(configuration.getBoolean(eq(Constants.SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE_KEY), anyBoolean())).thenReturn(true);
        when(configuration.getString(eq(Constants.SINK_KAFKA_LINGER_MS_KEY), anyString())).thenReturn("1000");
        Properties producerProperties = sinkOrchestrator.getProducerProperties(configuration);

        assertEquals(producerProperties.getProperty("compression.type"), "snappy");
        assertEquals(producerProperties.getProperty("max.request.size"), "20971520");
        assertEquals(producerProperties.getProperty("linger.ms"), "1000");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForInvalidLingerMs() throws Exception {
        when(configuration.getString(eq(Constants.SINK_KAFKA_BROKERS_KEY), anyString())).thenReturn("10.200.216.87:6668");
        when(configuration.getBoolean(eq(Constants.SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE_KEY), anyBoolean())).thenReturn(true);
        when(configuration.getString(eq(Constants.SINK_KAFKA_LINGER_MS_KEY), anyString())).thenReturn("abc");
        Assert.assertThrows("Expected Illegal ArgumentException", IllegalArgumentException.class,
                () -> sinkOrchestrator.getProducerProperties(configuration));
    }

    @Test
    public void shouldReturnSinkMetrics() {
        ArrayList<String> sinkType = new ArrayList<>();
        sinkType.add("influx");
        HashMap<String, List<String>> expectedMetrics = new HashMap<>();
        expectedMetrics.put("sink_type", sinkType);

        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("influx");

        sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator, daggerStatsDReporter);
        assertEquals(expectedMetrics, sinkOrchestrator.getTelemetry());
    }

    @Test
    public void shouldReturnBigQuerySink() {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("bigquery");
        when(configuration.getString("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "")).thenReturn("some.class");
        when(configuration.getParam()).thenReturn(ParameterTool.fromMap(Collections.emptyMap()));
        Sink sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator, daggerStatsDReporter);
        assertThat(sinkFunction, instanceOf(BigQuerySink.class));
    }
}
