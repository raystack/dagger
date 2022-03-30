package io.odpf.dagger.core.streamtype;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.dagger.core.streamtype.KafkaSourceJsonSchema.KafkaSourceJsonTypeBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Properties;

import static io.odpf.dagger.common.serde.DataTypes.JSON;
import static io.odpf.dagger.core.source.SourceName.KAFKA;
import static io.odpf.dagger.core.source.SourceName.PARQUET;
import static io.odpf.dagger.core.source.SourceType.BOUNDED;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaSourceJsonSchemaTest {
    @Mock
    private StreamConfig streamConfig;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnTrueIfTheStreamTypeCanBeBuiltFromConfigs() {
        KafkaSourceJsonTypeBuilder kafkaSourceJsonTypeBuilder = new KafkaSourceJsonTypeBuilder(streamConfig, configuration);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        boolean canBuild = kafkaSourceJsonTypeBuilder.canBuild();

        assertTrue(canBuild);
    }

    @Test
    public void shouldReturnFalseIfTheSourceNameIsNotSupported() {
        KafkaSourceJsonTypeBuilder kafkaSourceJsonTypeBuilder = new KafkaSourceJsonTypeBuilder(streamConfig, configuration);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(PARQUET, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        boolean canBuild = kafkaSourceJsonTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfMultipleBackToBackSourcesAreConfigured() {
        KafkaSourceJsonTypeBuilder kafkaSourceJsonTypeBuilder = new KafkaSourceJsonTypeBuilder(streamConfig, configuration);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, BOUNDED), new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        boolean canBuild = kafkaSourceJsonTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfTheSourceTypeIsNotSupported() {
        KafkaSourceJsonTypeBuilder kafkaSourceJsonTypeBuilder = new KafkaSourceJsonTypeBuilder(streamConfig, configuration);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        boolean canBuild = kafkaSourceJsonTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldReturnFalseIfTheSchemaTypeIsNotSupported() {
        KafkaSourceJsonTypeBuilder kafkaSourceJsonTypeBuilder = new KafkaSourceJsonTypeBuilder(streamConfig, configuration);
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(KAFKA, UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        boolean canBuild = kafkaSourceJsonTypeBuilder.canBuild();

        assertFalse(canBuild);
    }

    @Test
    public void shouldBuildAStreamTypeWithKafkaSourceAndJsonSchemaType() {
        KafkaSourceJsonTypeBuilder kafkaSourceJsonTypeBuilder = new KafkaSourceJsonTypeBuilder(streamConfig, configuration);
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getJsonSchema()).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");
        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getStartingOffset()).thenReturn(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf("LATEST")));
        when(streamConfig.getSchemaTable()).thenReturn("test-table");

        StreamType<Row> streamType = kafkaSourceJsonTypeBuilder.build();

        assertTrue(streamType.getSource() instanceof KafkaSource);
        assertEquals("test-table", streamType.getStreamName());
        assertTrue(streamType.getDeserializer() instanceof JsonDeserializer);
        assertEquals(JSON, streamType.getInputDataType());
    }
}