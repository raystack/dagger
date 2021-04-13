package io.odpf.dagger.sink;

import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.sink.influx.InfluxRowSink;
import io.odpf.dagger.sink.log.LogSink;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static io.odpf.dagger.utils.Constants.*;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SinkOrchestratorTest {

    private Configuration configuration;

    private StencilClientOrchestrator stencilClientOrchestrator;
    private SinkOrchestrator sinkOrchestrator;

    @Before
    public void setup() {
        initMocks(this);
        configuration = mock(Configuration.class, withSettings().serializable());
        when(configuration.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT)).thenReturn(STENCIL_CONFIG_REFRESH_CACHE_DEFAULT);
        when(configuration.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT)).thenReturn(STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn(STENCIL_URL_DEFAULT);

        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        sinkOrchestrator = new SinkOrchestrator();
    }

    @Test
    public void shouldGiveInfluxSinkWhenConfiguredToUseInflux() throws Exception {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("influx");

        Function sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);

        assertThat(sinkFunction, instanceOf(InfluxRowSink.class));
    }

    @Test
    public void shouldGiveLogSinkWhenConfiguredToUseLog() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("log");

        Function sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);

        assertThat(sinkFunction, instanceOf(LogSink.class));
    }

    @Test
    public void shouldGiveInfluxWhenConfiguredToUseNothing() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("");
        Function sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);

        assertThat(sinkFunction, instanceOf(InfluxRowSink.class));
    }


    @Test
    public void shouldSetKafkaProducerConfigurations() throws Exception {
        when(configuration.getString(eq(OUTPUT_KAFKA_BROKER), anyString())).thenReturn("10.200.216.87:6668");
        when(configuration.getBoolean(eq(PRODUCE_LARGE_MESSAGE_KEY), anyBoolean())).thenReturn(true);
        Properties producerProperties = sinkOrchestrator.getProducerProperties(configuration);

        assertEquals(producerProperties.getProperty("compression.type"), "snappy");
        assertEquals(producerProperties.getProperty("max.request.size"), "20971520");
    }

    @Test
    public void shouldGiveKafkaProducerWhenConfiguredToUseKafkaSink() throws Exception {
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("kafka");
        when(configuration.getString(eq("OUTPUT_PROTO_MESSAGE"), anyString())).thenReturn("output_proto");
        when(configuration.getString(eq("OUTPUT_KAFKA_BROKER"), anyString())).thenReturn("output_broker:2667");
        when(configuration.getString(eq("OUTPUT_KAFKA_TOPIC"), anyString())).thenReturn("output_topic");

        Function sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);

        assertThat(sinkFunction, instanceOf(FlinkKafkaProducerCustom.class));
    }

    @Test
    public void shouldReturnSinkMetrics() {
        ArrayList<String> sinkType = new ArrayList<>();
        sinkType.add("influx");
        HashMap<String, List<String>> expectedMetrics = new HashMap<>();
        expectedMetrics.put("sink_type", sinkType);

        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("influx");

        sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);
        Assert.assertEquals(expectedMetrics, sinkOrchestrator.getTelemetry());
    }


    @Test
    public void shouldReturnOutputKafkaMetrics() {
        ArrayList<String> sinkType = new ArrayList<>();
        sinkType.add("kafka");
        ArrayList<String> outputTopic = new ArrayList<>();
        outputTopic.add("test_topic");
        ArrayList<String> outputStream = new ArrayList<>();
        outputStream.add("test_output_stream");
        ArrayList<String> outputProto = new ArrayList<>();
        outputProto.add("test_output_proto");

        HashMap<String, List<String>> expectedMetrics = new HashMap<>();
        expectedMetrics.put("sink_type", sinkType);
        expectedMetrics.put("output_topic", outputTopic);
        expectedMetrics.put("output_proto", outputProto);
        expectedMetrics.put("output_stream", outputStream);


        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("kafka");
        when(configuration.getString(eq("OUTPUT_PROTO_MESSAGE"), any())).thenReturn("test_output_proto");
        when(configuration.getString(eq("OUTPUT_KAFKA_BROKER"), anyString())).thenReturn("output_broker:2667");
        when(configuration.getString(eq("OUTPUT_STREAM"), anyString())).thenReturn("test_output_stream");
        when(configuration.getString(eq("OUTPUT_KAFKA_TOPIC"), anyString())).thenReturn("test_topic");

        sinkOrchestrator.getSink(configuration, new String[]{}, stencilClientOrchestrator);
        Assert.assertEquals(expectedMetrics, sinkOrchestrator.getTelemetry());
    }
}
