package io.odpf.dagger.core.stream;

import com.google.gson.JsonSyntaxException;
import io.odpf.dagger.common.configuration.Configuration;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static io.odpf.dagger.core.utils.Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamConfigTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldParseStreamConfigs() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        Assert.assertEquals(1, streamConfigs.length);
    }

    @Test
    public void shouldSetConfigurationsFromJsonStreamConfig() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        StreamConfig currConfig = streamConfigs[0];
        Assert.assertEquals("false", currConfig.getAutoCommitEnabled());
        Assert.assertEquals("latest", currConfig.getAutoOffsetReset());
        Assert.assertEquals("PROTO", currConfig.getStreamDataType());
        Assert.assertEquals("dummy-consumer-group", currConfig.getConsumerGroup());
        Assert.assertEquals("41", currConfig.getEventTimestampFieldIndex());
        Assert.assertEquals("test-topic", currConfig.getKafkaTopic());
        Assert.assertEquals("data_stream", currConfig.getSchemaTable());
        Assert.assertEquals("local-kafka-stream", currConfig.getSourceKafkaName());
    }

    @Test
    public void shouldParseMultipleStreamsFromStreamConfigJson() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" }, { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"SOURCE_DATATYPE\": \"JSON\", \"SOURCE_JSON_SCHEMA\": \"{ \\\"$schema\\\": \\\"https://json-schema.org/draft/2020-12/schema\\\", \\\"$id\\\": \\\"https://example.com/product.schema.json\\\", \\\"title\\\": \\\"Product\\\", \\\"description\\\": \\\"A product from Acme's catalog\\\", \\\"type\\\": \\\"object\\\", \\\"properties\\\": { \\\"id\\\": { \\\"description\\\": \\\"The unique identifier for a product\\\", \\\"type\\\": \\\"string\\\" }, \\\"time\\\": { \\\"description\\\": \\\"event timestamp of the event\\\", \\\"type\\\": \\\"string\\\", \\\"format\\\" : \\\"date-time\\\" } }, \\\"required\\\": [ \\\"id\\\", \\\"time\\\" ] }\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        Assert.assertEquals(2, streamConfigs.length);
    }


    @Test
    public void shouldParseKafkaProperties() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        when(configuration.getBoolean(SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)).thenReturn(false);
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");
        kafkaPropMap.put("auto.offset.reset", "latest");
        kafkaPropMap.put("auto.commit.enable", "");


        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        Assert.assertEquals(properties, streamConfigs[0].getKafkaProps(configuration));
    }

    @Test
    public void shouldAddAdditionalKafkaConfigToKafkaProperties() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        when(configuration.getBoolean(SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)).thenReturn(true);
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");
        kafkaPropMap.put("auto.offset.reset", "latest");
        kafkaPropMap.put("auto.commit.enable", "");
        kafkaPropMap.put("max.partition.fetch.bytes", "5242880");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        Assert.assertEquals(properties, streamConfigs[0].getKafkaProps(configuration));
    }

    @Test
    public void shouldSetValidDataTypeIfNotGiven() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        Assert.assertEquals("PROTO", streamConfigs[0].getStreamDataType());
    }

    @Test
    public void shouldGetTopicPattern() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        Assert.assertEquals(Pattern.compile("test-topic").pattern(), streamConfigs[0].getTopicPattern().pattern());
    }

    @Test
    public void shouldGetOffsetResetStrategy() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        OffsetResetStrategy autoOffsetResetStrategy = streamConfigs[0].getStartingOffset().getAutoOffsetResetStrategy();
        Assert.assertEquals(OffsetResetStrategy.valueOf("LATEST"), autoOffsetResetStrategy);
    }

    @Test
    public void shouldThrowInCaseOfInvalidStream() {
        thrown.expect(JsonSyntaxException.class);
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \\\"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig.parse(configuration);
    }
}
