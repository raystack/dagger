package io.odpf.dagger.core.source;

import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.streamtype.KafkaSourceJsonSchemaStreamType;
import io.odpf.dagger.core.streamtype.KafkaSourceProtoSchemaStreamType;
import io.odpf.dagger.core.streamtype.ParquetSourceProtoSchemaStreamType;
import io.odpf.dagger.core.streamtype.StreamType;
import io.odpf.stencil.client.StencilClient;
import com.google.gson.JsonSyntaxException;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static io.odpf.dagger.common.serde.DataTypes.JSON;
import static io.odpf.dagger.common.serde.DataTypes.PROTO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamsFactoryTest {
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateSingleStreamTypeFromSingleStreamConfig() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"test-group-13\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\","
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        List<StreamType<Row>> streamTypes = StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator);

        assertEquals(1, streamTypes.size());
    }

    @Test
    public void shouldBeAbleToCreateStreamTypeWithKafkaSourceAndProtoSchemaAndProtoDeserializer() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"test-group-13\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\","
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        List<StreamType<Row>> streamTypes = StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator);
        StreamType<Row> actualStreamType = streamTypes.get(0);

        assertTrue(actualStreamType instanceof KafkaSourceProtoSchemaStreamType);
        assertEquals(PROTO, actualStreamType.getInputDataType());
        assertTrue(actualStreamType.getSource() instanceof KafkaSource);
        assertTrue(actualStreamType.getDeserializer() instanceof ProtoDeserializer);
        assertEquals("data_stream", actualStreamType.getStreamName());
    }

    @Test
    public void shouldCreateKafkaSourceProtoSchemaStreamTypeIfInputDataTypeNotSpecifiedInConfigs() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"test-group-13\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\","
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        List<StreamType<Row>> streamTypes = StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator);
        StreamType<Row> actualStreamType = streamTypes.get(0);

        assertTrue(actualStreamType instanceof KafkaSourceProtoSchemaStreamType);
        assertEquals(PROTO, actualStreamType.getInputDataType());
    }

    @Test
    public void shouldCreateStreamTypeOfKafkaSourceIfSourceDetailsNotSpecifiedInConfigsForProtoSchema() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"test-group-13\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\","
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        List<StreamType<Row>> streamTypes = StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator);
        StreamType<Row> actualStreamType = streamTypes.get(0);

        assertTrue(actualStreamType instanceof KafkaSourceProtoSchemaStreamType);
        assertEquals(PROTO, actualStreamType.getInputDataType());
        assertTrue(actualStreamType.getSource() instanceof KafkaSource);
        assertTrue(actualStreamType.getDeserializer() instanceof ProtoDeserializer);
        assertEquals("data_stream", actualStreamType.getStreamName());
    }

    @Test
    public void shouldBeAbleToCreateStreamTypeWithKafkaSourceAndJSONSchemaAndJSONDeserializer() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[{"
                + "\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\","
                + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA\"}],"
                + "\"INPUT_DATATYPE\": \"JSON\","
                + "\"INPUT_SCHEMA_JSON_SCHEMA\" : \"{ \\\"$schema\\\": \\\"https://json-schema.org/draft/2020-12/schema\\\", \\\"$id\\\": \\\"https://example.com/product.schema.json\\\", \\\"title\\\": \\\"Product\\\", \\\"description\\\": \\\"A product from Acme's catalog\\\", \\\"type\\\": \\\"object\\\", \\\"properties\\\": { \\\"id\\\": { \\\"description\\\": \\\"The unique identifier for a product\\\", \\\"type\\\": \\\"string\\\" }, \\\"time\\\": { \\\"description\\\": \\\"event timestamp of the event\\\", \\\"type\\\": \\\"string\\\", \\\"format\\\" : \\\"date-time\\\" } }, \\\"required\\\": [ \\\"id\\\", \\\"time\\\" ] }\","
                + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\","
                + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\","
                + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\","
                + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");

        List<StreamType<Row>> streamTypes = StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator);
        StreamType<Row> actualStreamType = streamTypes.get(0);

        assertTrue(actualStreamType instanceof KafkaSourceJsonSchemaStreamType);
        assertEquals(JSON, actualStreamType.getInputDataType());
        assertTrue(actualStreamType.getSource() instanceof KafkaSource);
        assertTrue(actualStreamType.getDeserializer() instanceof JsonDeserializer);
        assertEquals("data_stream", actualStreamType.getStreamName());
    }

    @Test
    public void shouldCreateStreamTypeOfKafkaSourceIfSourceDetailsNotSpecifiedInConfigsForJSONSchema() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[{"
                + "\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic\","
                + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA\"}],"
                + "\"INPUT_DATATYPE\": \"JSON\","
                + "\"INPUT_SCHEMA_JSON_SCHEMA\" : \"{ \\\"$schema\\\": \\\"https://json-schema.org/draft/2020-12/schema\\\", \\\"$id\\\": \\\"https://example.com/product.schema.json\\\", \\\"title\\\": \\\"Product\\\", \\\"description\\\": \\\"A product from Acme's catalog\\\", \\\"type\\\": \\\"object\\\", \\\"properties\\\": { \\\"id\\\": { \\\"description\\\": \\\"The unique identifier for a product\\\", \\\"type\\\": \\\"string\\\" }, \\\"time\\\": { \\\"description\\\": \\\"event timestamp of the event\\\", \\\"type\\\": \\\"string\\\", \\\"format\\\" : \\\"date-time\\\" } }, \\\"required\\\": [ \\\"id\\\", \\\"time\\\" ] }\","
                + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\","
                + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\","
                + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\","
                + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" } ]");

        List<StreamType<Row>> streamTypes = StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator);
        StreamType<Row> actualStreamType = streamTypes.get(0);

        assertTrue(actualStreamType instanceof KafkaSourceJsonSchemaStreamType);
        assertEquals(JSON, actualStreamType.getInputDataType());
        assertTrue(actualStreamType.getSource() instanceof KafkaSource);
        assertTrue(actualStreamType.getDeserializer() instanceof JsonDeserializer);
        assertEquals("data_stream", actualStreamType.getStreamName());
    }

    @Test
    public void shouldBeAbleToCreateStreamTypeWithParquetSourceAndProtoSchemaAndSimpleGroupDeserializer() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-05/hr=09/\",  \"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-03/hr=14/\"],"
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        List<StreamType<Row>> streamTypes = StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator);
        StreamType<Row> actualStreamType = streamTypes.get(0);

        assertTrue(actualStreamType instanceof ParquetSourceProtoSchemaStreamType);
        assertEquals(PROTO, actualStreamType.getInputDataType());
        assertTrue(actualStreamType.getSource() instanceof FileSource);
        assertTrue(actualStreamType.getDeserializer() instanceof SimpleGroupDeserializer);
        assertEquals("data_stream", actualStreamType.getStreamName());
    }

    @Test
    public void shouldBeAbleToCreateMultipleStreamTypes() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream_1\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic-1\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"test-group-13\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\","
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"},"
                        + "{\"INPUT_SCHEMA_TABLE\": \"data_stream_2\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic-2\","
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA\"}],"
                        + "\"INPUT_DATATYPE\": \"JSON\","
                        + "\"INPUT_SCHEMA_JSON_SCHEMA\" : \"{ \\\"$schema\\\": \\\"https://json-schema.org/draft/2020-12/schema\\\", \\\"$id\\\": \\\"https://example.com/product.schema.json\\\", \\\"title\\\": \\\"Product\\\", \\\"description\\\": \\\"A product from Acme's catalog\\\", \\\"type\\\": \\\"object\\\", \\\"properties\\\": { \\\"id\\\": { \\\"description\\\": \\\"The unique identifier for a product\\\", \\\"type\\\": \\\"string\\\" }, \\\"time\\\": { \\\"description\\\": \\\"event timestamp of the event\\\", \\\"type\\\": \\\"string\\\", \\\"format\\\" : \\\"date-time\\\" } }, \\\"required\\\": [ \\\"id\\\", \\\"time\\\" ] }\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream\" }]");

        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        List<StreamType<Row>> streamTypes = StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator);

        assertEquals(2, streamTypes.size());

        assertTrue(streamTypes.get(0) instanceof KafkaSourceProtoSchemaStreamType);
        assertEquals(PROTO, streamTypes.get(0).getInputDataType());
        assertTrue(streamTypes.get(0).getSource() instanceof KafkaSource);
        assertTrue(streamTypes.get(0).getDeserializer() instanceof ProtoDeserializer);
        assertEquals("data_stream_1", streamTypes.get(0).getStreamName());

        assertTrue(streamTypes.get(1) instanceof KafkaSourceJsonSchemaStreamType);
        assertEquals(JSON, streamTypes.get(1).getInputDataType());
        assertTrue(streamTypes.get(1).getSource() instanceof KafkaSource);
        assertTrue(streamTypes.get(1).getDeserializer() instanceof JsonDeserializer);
        assertEquals("data_stream_2", streamTypes.get(1).getStreamName());
    }

    @Test
    public void shouldThrowErrorForInvalidStreamConfig() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \\\"EARLIEST_TIME_URL_FIRST,"
                        + "\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-05/hr=09/\",  \"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-03/hr=14/\"],"
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        assertThrows(JsonSyntaxException.class,
                () -> StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator));
    }

    @Test
    public void shouldThrowDaggerConfigurationExceptionWhenNoSupportedStreamType() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"INPUT_DATATYPE\": \"JSON\","
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \"EARLIEST_TIME_URL_FIRST\","
                        + "\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-05/hr=09/\",  \"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-03/hr=14/\"],"
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        assertThrows(DaggerConfigurationException.class,
                () -> StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator));
    }

    @Test
    public void shouldThrowNullPointerIfStreamConfigIsNotGiven() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("");

        assertThrows(NullPointerException.class,
                () -> StreamsFactory.getStreamTypes(configuration, stencilClientOrchestrator));
    }
}
