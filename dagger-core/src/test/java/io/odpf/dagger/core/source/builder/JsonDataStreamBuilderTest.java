package io.odpf.dagger.core.source.builder;

import io.odpf.dagger.core.source.SourceDetails;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.source.Stream;
import io.odpf.dagger.core.source.StreamConfig;
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
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class JsonDataStreamBuilderTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Configuration configuration;

    @Mock
    private StreamConfig streamConfig;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessJSONStream() {
        when(streamConfig.getDataType()).thenReturn("JSON");
        JsonDataStreamBuilder jsonDataStreamBuilder = new JsonDataStreamBuilder(streamConfig, configuration);

        Assert.assertTrue(jsonDataStreamBuilder.canBuild());
    }

    @Test
    public void shouldParseDataTypeFromStreamConfig() {
        when(streamConfig.getDataType()).thenReturn("JSON");
        JsonDataStreamBuilder jsonDataStreamBuilder = new JsonDataStreamBuilder(streamConfig, configuration);

        Assert.assertEquals(DataTypes.JSON, jsonDataStreamBuilder.getInputDataType());
    }

    @Test
    public void shouldIgnoreProtoStream() {
        when(streamConfig.getDataType()).thenReturn("PROTO");
        JsonDataStreamBuilder jsonDataStreamBuilder = new JsonDataStreamBuilder(streamConfig, configuration);

        Assert.assertFalse(jsonDataStreamBuilder.canBuild());
    }

    @Test
    public void shouldBuildJSONStreamIfConfigured() {
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");
        when(streamConfig.getJsonSchema()).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("1");

        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getSchemaTable()).thenReturn("test-table");

        JsonDataStreamBuilder jsonDataStreamBuilder = new JsonDataStreamBuilder(streamConfig, configuration);

        Stream build = jsonDataStreamBuilder.build();

        Assert.assertEquals(DataTypes.JSON, build.getInputDataType());
        Assert.assertTrue(build.getSource() instanceof KafkaSource);
        Assert.assertEquals("test-table", build.getStreamName());
    }

    @Test
    public void shouldAddMetricsSpecificToKafkaSource() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getKafkaTopicNames()).thenReturn("test-topic");
        when(streamConfig.getKafkaName()).thenReturn("test-kafka");
        JsonDataStreamBuilder jsonDataStreamBuilder = new JsonDataStreamBuilder(streamConfig, configuration);
        jsonDataStreamBuilder.addTelemetry();

        Map<String, List<String>> metrics = jsonDataStreamBuilder.getMetrics();

        Assert.assertEquals(Arrays.asList(new String[]{"test-topic"}), metrics.get("input_topic"));
        Assert.assertEquals(Arrays.asList(new String[]{"test-kafka"}), metrics.get("input_stream"));
    }

    @Test
    public void shouldFailToCreateStreamIfSomeConfigsAreMissing() {
        thrown.expect(NullPointerException.class);
        JsonDataStreamBuilder jsonDataStreamBuilder = new JsonDataStreamBuilder(streamConfig, configuration);

        jsonDataStreamBuilder.build();
    }

    @Test
    public void shouldBeAbleToBuildStreamConsistingOfASingleSource() {
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getDataType()).thenReturn("JSON");
        when(streamConfig.getJsonSchema()).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("1");
        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getSchemaTable()).thenReturn("test-table");
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});

        JsonDataStreamBuilder jsonDataStreamBuilder = new JsonDataStreamBuilder(streamConfig, configuration);

        Stream build = jsonDataStreamBuilder.build();
        SourceDetails[] sourceDetails = build.getSourceDetails();
        Source source = build.getSource();

        assertEquals(DataTypes.JSON, build.getInputDataType());
        assertEquals(1, sourceDetails.length);
        assertEquals(UNBOUNDED, sourceDetails[0].getSourceType());
        assertEquals(KAFKA, sourceDetails[0].getSourceName());
        assertEquals("test-table", build.getStreamName());
        assertTrue(source instanceof KafkaSource);
    }
}
