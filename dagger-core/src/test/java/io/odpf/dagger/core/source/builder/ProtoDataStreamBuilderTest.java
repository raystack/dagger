package io.odpf.dagger.core.source.builder;

import io.odpf.dagger.core.source.*;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import io.odpf.stencil.client.StencilClient;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.odpf.dagger.core.source.SourceName.KAFKA;
import static io.odpf.dagger.core.source.SourceName.PARQUET;
import static io.odpf.dagger.core.source.SourceType.BOUNDED;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;
import static io.odpf.dagger.core.source.parquet.SourceParquetReadOrderStrategy.EARLIEST_INDEX_FIRST;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoDataStreamBuilderTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessProtoStream() {
        when(streamConfig.getDataType()).thenReturn("PROTO");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        assertTrue(protoDataStreamBuilder.canBuild());
    }

    @Test
    public void shouldParseDataTypeFromStreamConfig() {
        when(streamConfig.getDataType()).thenReturn("PROTO");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        assertEquals(DataTypes.PROTO, protoDataStreamBuilder.getInputDataType());
    }

    @Test
    public void shouldIgnoreJsonStream() {
        when(streamConfig.getDataType()).thenReturn("JSON");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        Assert.assertFalse(protoDataStreamBuilder.canBuild());
    }

    @Test
    public void shouldBuildProtoStreamIfConfigured() {
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("1");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getSchemaTable()).thenReturn("test-table");

        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        Stream build = protoDataStreamBuilder.build();

        assertEquals(DataTypes.PROTO, build.getInputDataType());
        assertTrue(build.getSource() instanceof KafkaSource);
        assertEquals("test-table", build.getStreamName());
    }

    @Test
    public void shouldAddMetricsSpecificToKafkaSource() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getKafkaTopicNames()).thenReturn("test-topic");
        when(streamConfig.getProtoClass()).thenReturn("test-class");
        when(streamConfig.getKafkaName()).thenReturn("test-kafka");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);
        protoDataStreamBuilder.addTelemetry();

        Map<String, List<String>> metrics = protoDataStreamBuilder.getMetrics();

        assertEquals(Arrays.asList(new String[]{"test-topic"}), metrics.get("input_topic"));
        assertEquals(Arrays.asList(new String[]{"test-class"}), metrics.get("input_proto"));
        assertEquals(Arrays.asList(new String[]{"test-kafka"}), metrics.get("input_stream"));
    }

    @Test
    public void shouldAddMetricsSpecificToParquetSource() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(PARQUET, BOUNDED)});
        when(streamConfig.getProtoClass()).thenReturn("test-class");
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);
        protoDataStreamBuilder.addTelemetry();

        Map<String, List<String>> expectedMetrics = protoDataStreamBuilder.getMetrics();

        assertEquals(1, expectedMetrics.size());
        assertEquals(Arrays.asList(new String[]{"test-class"}), expectedMetrics.get("input_proto"));
    }

    @Test
    public void shouldFailToCreateStreamIfSomeConfigsAreMissing() {
        thrown.expect(NullPointerException.class);
        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        protoDataStreamBuilder.build();
    }

    @Test
    public void shouldBeAbleToBuildStreamConsistingOfASingleSource() {
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("1");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getSchemaTable()).thenReturn("test-table");

        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        Stream build = protoDataStreamBuilder.buildStream();
        SourceDetails[] sourceDetails = build.getSourceDetails();
        Source source = build.getSource();

        assertEquals(DataTypes.PROTO, build.getInputDataType());
        assertEquals(1, sourceDetails.length);
        assertEquals(UNBOUNDED, sourceDetails[0].getSourceType());
        assertEquals(KAFKA, sourceDetails[0].getSourceName());
        assertEquals("test-table", build.getStreamName());
        assertTrue(source instanceof KafkaSource);
    }

    @Test
    public void shouldThrowExceptionWhenMultipleSourcesAreConfiguredInStreamConfig() {
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("1");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(streamConfig.getParquetFilePaths()).thenReturn(new String[]{"gs://something", "gs://anything"});
        when(streamConfig.getParquetFilesReadOrderStrategy()).thenReturn(EARLIEST_INDEX_FIRST);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]
                {new SourceDetails(PARQUET, BOUNDED), new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getSchemaTable()).thenReturn("test-table");

        ProtoDataStreamBuilder protoDataStreamBuilder = new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration);

        IllegalConfigurationException exception = assertThrows(IllegalConfigurationException.class,
                protoDataStreamBuilder::buildStream);
        assertEquals("Invalid stream configuration: Multiple back to back data sources is not supported yet.", exception.getMessage());
    }
}
