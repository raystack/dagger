package com.gotocompany.dagger.core.source.config;

import com.google.gson.JsonSyntaxException;
import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.core.source.config.models.*;
import com.gotocompany.dagger.core.source.parquet.SourceParquetReadOrderStrategy;
import com.gotocompany.dagger.core.source.parquet.SourceParquetSchemaMatchStrategy;
import com.gotocompany.dagger.core.utils.Constants;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.gotocompany.dagger.common.core.Constants.INPUT_STREAMS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
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
        when(configuration.getBoolean(Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)).thenReturn(false);
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
    public void shouldParseKafkaPropertiesWithSSLConfigurations() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL\": \"SSL\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEY_PASSWORD\": \"test-key-pass\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_PROTOCOL\": \"SSL\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_LOCATION\": \"test-keystore-location\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_PASSWORD\": \"test-keystore-pass\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_TYPE\": \"JKS\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_LOCATION\": \"test-truststore-location\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_PASSWORD\": \"test-truststore-pass\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_TYPE\": \"JKS\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        when(configuration.getBoolean(Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)).thenReturn(false);
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");
        kafkaPropMap.put("auto.offset.reset", "latest");
        kafkaPropMap.put("auto.commit.enable", "");
        kafkaPropMap.put("ssl.keystore.password", "test-keystore-pass");
        kafkaPropMap.put("ssl.keystore.type", "JKS");
        kafkaPropMap.put("ssl.keystore.location", "test-keystore-location");
        kafkaPropMap.put("ssl.protocol", "SSL");
        kafkaPropMap.put("ssl.key.password", "test-key-pass");
        kafkaPropMap.put("ssl.truststore.type", "JKS");
        kafkaPropMap.put("ssl.truststore.location", "test-truststore-location");
        kafkaPropMap.put("ssl.truststore.password", "test-truststore-pass");
        kafkaPropMap.put("security.protocol", "SSL");



        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        assertEquals(properties, streamConfigs[0].getKafkaProps(configuration));
    }

    @Test
    public void shouldParseMultipleStreamsFromStreamConfigWithSSLConfigurations() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL\": \"SSL\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEY_PASSWORD\": \"test-key-pass\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_PROTOCOL\": \"SSL\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_LOCATION\": \"test-keystore-location\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_PASSWORD\": \"test-keystore-pass\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_TYPE\": \"JKS\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_LOCATION\": \"test-truststore-location\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_PASSWORD\": \"test-truststore-pass\", \"SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_TYPE\": \"JKS\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" }, {\"INPUT_SCHEMA_TABLE\": \"data_stream_1\", \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_DATATYPE\": \"JSON\", \"INPUT_SCHEMA_JSON_SCHEMA\": \"{ \\\"$schema\\\": \\\"https://json-schema.org/draft/2020-12/schema\\\", \\\"$id\\\": \\\"https://example.com/product.schema.json\\\", \\\"title\\\": \\\"Product\\\", \\\"description\\\": \\\"A product from Acme's catalog\\\", \\\"type\\\": \\\"object\\\", \\\"properties\\\": { \\\"id\\\": { \\\"description\\\": \\\"The unique identifier for a product\\\", \\\"type\\\": \\\"string\\\" }, \\\"time\\\": { \\\"description\\\": \\\"event timestamp of the event\\\", \\\"type\\\": \\\"string\\\", \\\"format\\\" : \\\"date-time\\\" } }, \\\"required\\\": [ \\\"id\\\", \\\"time\\\" ] }\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
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
        assertEquals("SSL", currConfig.getSecurityProtocol());
        assertEquals("localhost:9092", currConfig.getBootstrapServers());
        assertEquals("SSL", currConfig.getSslProtocol());
        assertEquals("test-key-pass", currConfig.getSslKeyPassword());
        assertEquals("test-keystore-location", currConfig.getSslKeystoreLocation());
        assertEquals("test-keystore-pass", currConfig.getSslKeystorePassword());
        assertEquals("JKS", currConfig.getSslKeystoreType());
        assertEquals("test-truststore-location", currConfig.getSslTruststoreLocation());
        assertEquals("test-truststore-pass", currConfig.getSslTruststorePassword());
        assertEquals("JKS", currConfig.getSslTruststoreType());

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
    public void shouldParseKafkaPropertiesWithSASLConfig() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\",\"SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL\": \"SASL_PLAINTEXT\",\"SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM\":\"SCRAM-SHA-512\",\"SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG\":\"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"username\\\" password=\\\"password\\\";\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        when(configuration.getBoolean(Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)).thenReturn(false);
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");
        kafkaPropMap.put("auto.offset.reset", "latest");
        kafkaPropMap.put("auto.commit.enable", "");
        kafkaPropMap.put("sasl.mechanism", "SCRAM-SHA-512");
        kafkaPropMap.put("security.protocol", "SASL_PLAINTEXT");
        kafkaPropMap.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        assertEquals(properties, streamConfigs[0].getKafkaProps(configuration));
    }

    @Test
    public void shouldParseMultipleStreamsFromStreamConfigWithSASLConfig() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\",\"SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL\": \"SASL_PLAINTEXT\",\"SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM\":\"SCRAM-SHA-512\",\"SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG\":\"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"username\\\" password=\\\"password\\\";\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"false\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" }, {\"INPUT_SCHEMA_TABLE\": \"data_stream_1\", \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_DATATYPE\": \"JSON\", \"INPUT_SCHEMA_JSON_SCHEMA\": \"{ \\\"$schema\\\": \\\"https://json-schema.org/draft/2020-12/schema\\\", \\\"$id\\\": \\\"https://example.com/product.schema.json\\\", \\\"title\\\": \\\"Product\\\", \\\"description\\\": \\\"A product from Acme's catalog\\\", \\\"type\\\": \\\"object\\\", \\\"properties\\\": { \\\"id\\\": { \\\"description\\\": \\\"The unique identifier for a product\\\", \\\"type\\\": \\\"string\\\" }, \\\"time\\\": { \\\"description\\\": \\\"event timestamp of the event\\\", \\\"type\\\": \\\"string\\\", \\\"format\\\" : \\\"date-time\\\" } }, \\\"required\\\": [ \\\"id\\\", \\\"time\\\" ] }\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
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
        assertEquals("SCRAM-SHA-512", currConfig.getSaslMechanism());
        assertEquals("SASL_PLAINTEXT", currConfig.getSecurityProtocol());
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";", currConfig.getSaslJaasConfig());

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
    public void shouldAddAdditionalKafkaConfigToKafkaProperties() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[ { \"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\", \"INPUT_SCHEMA_TABLE\": \"data_stream\", \"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\", \"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\", \"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\", \"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\", \"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\", \"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");
        when(configuration.getBoolean(Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)).thenReturn(true);
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
                        + "{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA_SOURCE\"}],"
                        + "\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"]"
                        + "}]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        SourceDetails[] sourceDetails = streamConfigs[0].getSourceDetails();
        Assert.assertEquals(SourceType.valueOf("BOUNDED"), sourceDetails[0].getSourceType());
        Assert.assertEquals(SourceName.valueOf("PARQUET_SOURCE"), sourceDetails[0].getSourceName());
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
        Assert.assertEquals(SourceType.UNBOUNDED, sourceDetails[0].getSourceType());
        Assert.assertEquals(SourceName.KAFKA_CONSUMER, sourceDetails[0].getSourceName());
    }

    @Test
    public void shouldGetEarliestTimeUrlStrategyAsParquetReadOrderStrategyWhenNotGiven() {
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
        SourceParquetReadOrderStrategy actualReadOrderStrategy = streamConfigs[0].getParquetFilesReadOrderStrategy();

        Assert.assertEquals(SourceParquetReadOrderStrategy.EARLIEST_TIME_URL_FIRST, actualReadOrderStrategy);
    }

    @Test
    public void shouldGetParquetSourceProperties() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\""
                        + "}]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        Assert.assertArrayEquals(new String[]{"gs://some-parquet-path", "gs://another-parquet-path"}, streamConfigs[0].getParquetFilePaths());
        assertEquals(SourceParquetReadOrderStrategy.valueOf("EARLIEST_TIME_URL_FIRST"), streamConfigs[0].getParquetFilesReadOrderStrategy());
        Assert.assertEquals(SourceParquetSchemaMatchStrategy.valueOf("BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH"), streamConfigs[0].getParquetSchemaMatchStrategy());
    }

    @Test
    public void shouldParseParquetFileDateRange() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_FILE_DATE_RANGE\":\"2022-02-13T14:00:00, 2022-02-13T18:00:00Z\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\"}]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        TimeRangePool parquetFileDateRange = streamConfigs[0].getParquetFileDateRange();

        List<TimeRange> timeRanges = parquetFileDateRange.getTimeRanges();

        assertEquals(1644760800L, timeRanges.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775200L, timeRanges.get(0).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldReturnEmptyTimeRangeIfParquetFileDateRangeNotGiven() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\"}]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        assertNull(streamConfigs[0].getParquetFileDateRange());
    }

    @Test
    public void shouldThrowRuntimeExceptionIfSourceDetailsArrayContainsInvalidData() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": "
                        + "[null, {\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET_SOURCE\"}]"
                        + "}]");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> StreamConfig.parse(configuration));
        assertEquals("One or more elements inside SOURCE_DETAILS is either null or invalid.", exception.getMessage());
    }

    @Test
    public void shouldThrowRuntimeExceptionIfSourceDetailsArrayHasMissingSourceName() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": "
                        + "[{\"SOURCE_TYPE\": \"BOUNDED\"}]"
                        + "}]");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> StreamConfig.parse(configuration));
        assertEquals("One or more elements inside SOURCE_DETAILS has null or invalid SourceName. "
                + "Check if it is a valid SourceName and ensure no trailing/leading whitespaces are present", exception.getMessage());
    }

    @Test
    public void shouldThrowRuntimeExceptionIfSourceDetailsArrayContainsInvalidSourceName() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": "
                        + "[{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"           KAFKA_SOURCE\"}]"
                        + "}]");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> StreamConfig.parse(configuration));
        assertEquals("One or more elements inside SOURCE_DETAILS has null or invalid SourceName. "
                + "Check if it is a valid SourceName and ensure no trailing/leading whitespaces are present", exception.getMessage());
    }

    @Test
    public void shouldThrowRuntimeExceptionIfSourceDetailsArrayHasMissingSourceType() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": "
                        + "[{\"SOURCE_NAME\": \"PARQUET_SOURCE\"}]"
                        + "}]");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> StreamConfig.parse(configuration));
        assertEquals("One or more elements inside SOURCE_DETAILS has null or invalid SourceType. Check if it "
                + "is a valid SourceType and ensure no trailing/leading whitespaces are present", exception.getMessage());
    }

    @Test
    public void shouldThrowRuntimeExceptionIfSourceDetailsArrayContainsInvalidSourceType() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": "
                        + "[{\"SOURCE_TYPE\": \"       BOUNDED\", \"SOURCE_NAME\": \"KAFKA_SOURCE\"}]"
                        + "}]");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> StreamConfig.parse(configuration));
        assertEquals("One or more elements inside SOURCE_DETAILS has null or invalid SourceType. Check if it "
                + "is a valid SourceType and ensure no trailing/leading whitespaces are present", exception.getMessage());
    }

    @Test
    public void shouldThrowRuntimeExceptionIfSourceDetailsSetToEmptyArray() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://some-parquet-path\", \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": []"
                        + "}]");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> StreamConfig.parse(configuration));
        assertEquals("SOURCE_DETAILS config is set to an empty array. Please check the documentation and specify "
                + "in a valid format.", exception.getMessage());
    }

    @Test
    public void shouldThrowRuntimeExceptionForParquetSourceIfSourceParquetFilePathsArrayContainsInvalidFilePath() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [null, \"gs://another-parquet-path\"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": "
                        + "[{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET_SOURCE\"}]"
                        + "}]");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> StreamConfig.parse(configuration));
        assertEquals("One or more file path inside SOURCE_PARQUET_FILE_PATHS is null.", exception.getMessage());
    }

    @Test
    public void shouldThrowRuntimeExceptionForParquetSourceIfSourceParquetFilePathsArrayIsNull() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": "
                        + "[{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET_SOURCE\"}]"
                        + "}]");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> StreamConfig.parse(configuration));
        assertEquals("SOURCE_PARQUET_FILE_PATHS is required for configuring a Parquet Data Source Stream, "
                + "but is set to null.", exception.getMessage());
    }

    @Test
    public void shouldTrimLeadingAndTrailingWhitespacesFromParquetFilePathsWhenParquetSourceConfigured() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"SOURCE_PARQUET_FILE_PATHS\": [\"   gs://some-parquet-path\", \"   gs://another-parquet-path   \"],"
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY\": \"BACKWARD_COMPATIBLE_SCHEMA_WITH_FAIL_ON_TYPE_MISMATCH\","
                        + "\"SOURCE_DETAILS\": "
                        + "[{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET_SOURCE\"},"
                        + "{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA_SOURCE\"}]"
                        + "}]");
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);

        Assert.assertArrayEquals(new String[]{"gs://some-parquet-path", "gs://another-parquet-path"}, streamConfigs[0].getParquetFilePaths());
    }
}
