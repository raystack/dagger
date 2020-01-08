package com.gojek.daggers.sink;

import com.gojek.daggers.sink.influx.InfluxRowSink;
import com.gojek.daggers.sink.log.LogSink;
import com.gojek.de.stencil.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class SinkOrchestratorTest {

    private static StencilClient stencilClient = StencilClientFactory.getClient();
    SinkOrchestrator sinkOrchestrator = new SinkOrchestrator();

    @Test
    public void shouldGiveInfluxSinkWhenConfiguredToUseInflux() throws Exception {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("influx");

        Function sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClient);

        assertThat(sinkFunction, instanceOf(InfluxRowSink.class));
    }

    @Test
    public void shouldGiveLogSinkWhenConfiguredToUseLog() throws Exception {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("log");

        Function sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClient);

        assertThat(sinkFunction, instanceOf(LogSink.class));
    }

    @Test
    public void shouldGiveInfluxWhenConfiguredToUseNothing() throws Exception {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("");
        Function sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClient);

        assertThat(sinkFunction, instanceOf(InfluxRowSink.class));
    }

    @Test
    public void shouldGiveKafkaProducerWhenConfiguredToUseKafkaSink() throws Exception {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("kafka");
        when(configuration.getString(eq("OUTPUT_PROTO_CLASS_PREFIX"), anyString())).thenReturn("output_proto");
        when(configuration.getString(eq("OUTPUT_KAFKA_BROKER"), anyString())).thenReturn("output_broker:2667");
        when(configuration.getString(eq("OUTPUT_KAFKA_TOPIC"), anyString())).thenReturn("output_topic");
        // TODO: [PORTAL_MIGRATION] Remove this mock when migration to new portal is done
        when(configuration.getString(eq("PORTAL_VERSION"), anyString())).thenReturn("1");

        Function sinkFunction = sinkOrchestrator.getSink(configuration, new String[]{}, stencilClient);

        assertThat(sinkFunction, instanceOf(FlinkKafkaProducer010.class));
    }

    @Test
    public void shouldReturnSinkMetrics() {
        ArrayList<String> sinkType = new ArrayList<>();
        sinkType.add("influx");
        HashMap<String, List<String>> expectedMetrics = new HashMap<>();
        expectedMetrics.put("sink_type", sinkType);

        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("influx");

        sinkOrchestrator.getSink(configuration, new String[]{}, stencilClient);
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


        Configuration configuration = mock(Configuration.class);
        when(configuration.getString(eq("PORTAL_VERSION"), anyString())).thenReturn("2");
        when(configuration.getString(eq("SINK_TYPE"), anyString())).thenReturn("kafka");
        when(configuration.getString(eq("OUTPUT_PROTO_MESSAGE"), any())).thenReturn("test_output_proto");
        when(configuration.getString(eq("OUTPUT_KAFKA_BROKER"), anyString())).thenReturn("output_broker:2667");
        when(configuration.getString(eq("OUTPUT_STREAM"), anyString())).thenReturn("test_output_stream");
        when(configuration.getString(eq("OUTPUT_KAFKA_TOPIC"), anyString())).thenReturn("test_topic");

        sinkOrchestrator.getSink(configuration, new String[]{}, stencilClient);
        Assert.assertEquals(expectedMetrics, sinkOrchestrator.getTelemetry());
    }
}
