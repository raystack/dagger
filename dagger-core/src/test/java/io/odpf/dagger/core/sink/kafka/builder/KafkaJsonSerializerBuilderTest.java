package io.odpf.dagger.core.sink.kafka.builder;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.exceptions.serde.InvalidJSONSchemaException;
import io.odpf.dagger.core.utils.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaJsonSerializerBuilderTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString(Constants.SINK_KAFKA_TOPIC_KEY, "")).thenReturn("test-topic");
        when(configuration.getString(Constants.SINK_KAFKA_JSON_SCHEMA_KEY, "")).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");
        when(configuration.getString(Constants.SINK_KAFKA_STREAM_KEY, "")).thenReturn("test-kafka");
    }

    @Test
    public void shouldCreateJSONSerializer() {
        KafkaJsonSerializerBuilder kafkaJsonSerializerBuilder = new KafkaJsonSerializerBuilder(configuration);
        Assert.assertTrue(kafkaJsonSerializerBuilder.build() instanceof KafkaRecordSerializationSchema);
    }

    @Test
    public void shouldAddMetrics() {
        KafkaJsonSerializerBuilder kafkaJsonSerializerBuilder = new KafkaJsonSerializerBuilder(configuration);
        kafkaJsonSerializerBuilder.build();

        Map<String, List<String>> telemetry = kafkaJsonSerializerBuilder.getTelemetry();

        Assert.assertEquals(Arrays.asList(new String[]{"test-topic"}), telemetry.get("output_topic"));
        Assert.assertEquals(Arrays.asList(new String[]{"test-kafka"}), telemetry.get("output_stream"));
    }

    @Test
    public void shouldThrowErrorIfOutputJsonSchemaIsInvalid() {
        thrown.expect(InvalidJSONSchemaException.class);
        when(configuration.getString(Constants.SINK_KAFKA_JSON_SCHEMA_KEY, "")).thenReturn("{}");
        KafkaJsonSerializerBuilder kafkaJsonSerializerBuilder = new KafkaJsonSerializerBuilder(configuration);
        kafkaJsonSerializerBuilder.build();
    }

    @Test
    public void shouldThrowErrorIfOutputJsonSchemaIsEmpty() {
        thrown.expect(InvalidJSONSchemaException.class);
        when(configuration.getString(Constants.SINK_KAFKA_JSON_SCHEMA_KEY, "")).thenReturn("");
        KafkaJsonSerializerBuilder kafkaJsonSerializerBuilder = new KafkaJsonSerializerBuilder(configuration);
        kafkaJsonSerializerBuilder.build();
    }

    @Test
    public void shouldThrowErrorIfOutputJsonSchemaIsNull() {
        thrown.expect(NullPointerException.class);
        when(configuration.getString(Constants.SINK_KAFKA_JSON_SCHEMA_KEY, "")).thenReturn(null);
        KafkaJsonSerializerBuilder kafkaJsonSerializerBuilder = new KafkaJsonSerializerBuilder(configuration);
        kafkaJsonSerializerBuilder.build();
    }
}
