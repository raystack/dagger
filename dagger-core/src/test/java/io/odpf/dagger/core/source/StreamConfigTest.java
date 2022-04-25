package io.odpf.dagger.core.source;

import com.google.gson.JsonSyntaxException;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.parquet.SourceParquetReadOrderStrategy;
import io.odpf.dagger.core.source.parquet.SourceParquetSchemaMatchStrategy;
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
import static io.odpf.dagger.core.source.SourceName.KAFKA_CONSUMER;
import static io.odpf.dagger.core.source.SourceType.UNBOUNDED;
import static io.odpf.dagger.core.utils.Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY;
import static org.junit.Assert.assertEquals;
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

        assertEquals(1, streamConfigs.length);
    }

    @Test
    public void shouldSetConfigurationsFromJsonStreamConfig() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        StreamConfig currConfig = streamConfigs[0];
        assertEquals("false", currConfig.getAutoCommitEnable());
        assertEquals("latest", currConfig.getAutoOffsetReset());
        assertEquals("PROTO", currConfig.getDataType());
        assertEquals("dummy-consumer-group", currConfig.getConsumerGroupId());
        assertEquals("41", currConfig.getEventTimestampFieldIndex());
        assertEquals("test-topic", currConfig.getKafkaTopicNames());
        assertEquals("data_stream", currConfig.getSchemaTable());
        assertEquals("local-kafka-stream", currConfig.getKafkaName());
    }

    @Test
    public void shouldParseMultipleStreamsFromStreamConfigJson() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" }, {\"INPUT_SCHEMA_TABLE\": \"data_stream_1\", \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_DATATYPE\": \"JSON\", \"INPUT_SCHEMA_JSON_SCHEMA\": \"{ \\\"$schema\\\": \\\"https://json-schema.org/draft/2020-12/schema\\\", \\\"$id\\\": \\\"https://example.com/product.schema.json\\\", \\\"title\\\": \\\"Product\\\", \\\"description\\\": \\\"A product from Acme's catalog\\\", \\\"type\\\": \\\"object\\\", \\\"properties\\\": { \\\"id\\\": { \\\"description\\\": \\\"The unique identifier for a product\\\", \\\"type\\\": \\\"string\\\" }, \\\"time\\\": { \\\"description\\\": \\\"event timestamp of the event\\\", \\\"type\\\": \\\"string\\\", \\\"format\\\" : \\\"date-time\\\" } }, \\\"required\\\": [ \\\"id\\\", \\\"time\\\" ] }\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        assertEquals(2, streamConfigs.length);

        StreamConfig currConfig = streamConfigs[0];
        assertEquals("false", currConfig.getAutoCommitEnable());
        assertEquals("latest", currConfig.getAutoOffsetReset());
        assertEquals("PROTO", currConfig.getDataType());
        assertEquals("dummy-consumer-group", currConfig.getConsumerGroupId());
        assertEquals("41", currConfig.getEventTimestampFieldIndex());
        assertEquals("test-topic", currConfig.getKafkaTopicNames());
        assertEquals("data_stream", currConfig.getSchemaTable());
        assertEquals("local-kafka-stream", currConfig.getKafkaName());

        StreamConfig currConfigNext = streamConfigs[1];
        assertEquals("true", currConfigNext.getAutoCommitEnable());
        assertEquals("latest", currConfigNext.getAutoOffsetReset());
        assertEquals("JSON", currConfigNext.getDataType());
        assertEquals("dummy-consumer-group", currConfigNext.getConsumerGroupId());
        assertEquals("41", currConfigNext.getEventTimestampFieldIndex());
        assertEquals("test-topic", currConfigNext.getKafkaTopicNames());
        assertEquals("data_stream_1", currConfigNext.getSchemaTable());
        assertEquals("local-kafka-stream", currConfigNext.getKafkaName());
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

        assertEquals(properties, streamConfigs[0].getKafkaProps(configuration));
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

        assertEquals(properties, streamConfigs[0].getKafkaProps(configuration));
    }

    @Test
    public void shouldSetValidDataTypeIfNotGiven() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        assertEquals("PROTO", streamConfigs[0].getDataType());
    }

    @Test
    public void shouldSetValidDataTypeIfGiven() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ {\"INPUT_DATATYPE\": \"JSON\", \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET_Random\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        assertEquals("JSON", streamConfigs[0].getDataType());
    }

    @Test
    public void shouldGetTopicPattern() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        assertEquals(Pattern.compile("test-topic").pattern(), streamConfigs[0].getTopicPattern().pattern());
    }

    @Test
    public void shouldGetOffsetResetStrategy() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        OffsetResetStrategy autoOffsetResetStrategy = streamConfigs[0].getStartingOffset().getAutoOffsetResetStrategy();
        assertEquals(OffsetResetStrategy.valueOf("LATEST"), autoOffsetResetStrategy);
    }

    @Test
    public void shouldThrowInCaseOfInvalidStream() {
        thrown.expect(JsonSyntaxException.class);
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \\\"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        StreamConfig.parse(configuration);
    }

    @Test
    public void shouldGetSourceDetails() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{"
                        + "\"SOURCE_DETAILS\": "
                        + "[{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET_SOURCE\"},"
                        + "{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA_SOURCE\"}]"
                        + "}]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        SourceDetails[] sourceDetails = streamConfigs[0].getSourceDetails();
        assertEquals(SourceType.valueOf("BOUNDED"), sourceDetails[0].getSourceType());
        assertEquals(SourceName.valueOf("PARQUET_SOURCE"), sourceDetails[0].getSourceName());
        assertEquals(SourceType.valueOf("UNBOUNDED"), sourceDetails[1].getSourceType());
        assertEquals(SourceName.valueOf("KAFKA_SOURCE"), sourceDetails[1].getSourceName());
    }

    @Test
    public void shouldGetUnboundedKafkaConsumerAsSourceDetailsWhenNotGiven() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"test-group-13\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\","
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");

        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);
        SourceDetails[] sourceDetails = streamConfigs[0].getSourceDetails();

        assertEquals(1, sourceDetails.length);
        assertEquals(UNBOUNDED, sourceDetails[0].getSourceType());
        assertEquals(KAFKA_CONSUMER, sourceDetails[0].getSourceName());
    }

    @Test
    public void shouldGetParquetSourceProperties() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_BILLING_PROJECT\": \"data-project\","
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\""
                        + "}]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        Assert.assertArrayEquals(new String[]{"gs://some-parquet-path", "gs://another-parquet-path"}, streamConfigs[0].getParquetFilePaths());
        assertEquals("data-project", streamConfigs[0].getParquetBillingProject());
        assertEquals(SourceParquetReadOrderStrategy.valueOf("EARLIEST_TIME_URL_FIRST"), streamConfigs[0].getParquetFilesReadOrderStrategy());
        assertEquals(SourceParquetSchemaMatchStrategy.valueOf("BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH"), streamConfigs[0].getParquetSchemaMatchStrategy());
    }
}
