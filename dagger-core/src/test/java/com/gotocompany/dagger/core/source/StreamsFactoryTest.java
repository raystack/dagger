package com.gotocompany.dagger.core.source;

import com.gotocompany.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import com.gotocompany.dagger.core.source.flinkkafkaconsumer.FlinkKafkaConsumerDaggerSource;
import com.gotocompany.dagger.core.source.kafka.KafkaDaggerSource;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.client.StencilClient;
import com.google.gson.JsonSyntaxException;
import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.stencil.config.StencilConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;


import java.util.List;

import static com.gotocompany.dagger.common.core.Constants.INPUT_STREAMS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamsFactoryTest {
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private StencilConfig stencilConfig;

    @Mock
    private Configuration configuration;

    private final SerializedStatsDReporterSupplier statsDReporterSupplierMock = () -> mock(StatsDReporter.class);

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnListOfStreamsCreatedFromConfiguration() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream_1\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic-1\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"true\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"test-group-13\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream-1\","
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA_CONSUMER\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"},"
                        + "{\"INPUT_SCHEMA_TABLE\": \"data_stream_2\","
                        + "\"SOURCE_KAFKA_TOPIC_NAMES\": \"test-topic-2\","
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"UNBOUNDED\", \"SOURCE_NAME\": \"KAFKA_SOURCE\"}],"
                        + "\"INPUT_DATATYPE\": \"JSON\","
                        + "\"INPUT_SCHEMA_JSON_SCHEMA\" : \"{ \\\"$schema\\\": \\\"https://json-schema.org/draft/2020-12/schema\\\", \\\"$id\\\": \\\"https://example.com/product.schema.json\\\", \\\"title\\\": \\\"Product\\\", \\\"description\\\": \\\"A product from Acme's catalog\\\", \\\"type\\\": \\\"object\\\", \\\"properties\\\": { \\\"id\\\": { \\\"description\\\": \\\"The unique identifier for a product\\\", \\\"type\\\": \\\"string\\\" }, \\\"time\\\": { \\\"description\\\": \\\"event timestamp of the event\\\", \\\"type\\\": \\\"string\\\", \\\"format\\\" : \\\"date-time\\\" } }, \\\"required\\\": [ \\\"id\\\", \\\"time\\\" ] }\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\": \"localhost:9092\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\": \"\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\": \"latest\","
                        + "\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\": \"dummy-consumer-group\","
                        + "\"SOURCE_KAFKA_NAME\": \"local-kafka-stream-2\" }]");

        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilConfig.getCacheAutoRefresh()).thenReturn(false);
        when(stencilClientOrchestrator.createStencilConfig()).thenReturn(stencilConfig);
        List<Stream> streams = StreamsFactory.getStreams(configuration, stencilClientOrchestrator, statsDReporterSupplierMock);

        assertEquals(2, streams.size());
        assertTrue(streams.get(0).getDaggerSource() instanceof FlinkKafkaConsumerDaggerSource);
        assertEquals("data_stream_1", streams.get(0).getStreamName());
        assertTrue(streams.get(1).getDaggerSource() instanceof KafkaDaggerSource);
        assertEquals("data_stream_2", streams.get(1).getStreamName());
    }

    @Test
    public void shouldThrowErrorForInvalidStreamConfig() {
        when(configuration.getString(INPUT_STREAMS, ""))
                .thenReturn("[{\"INPUT_SCHEMA_TABLE\": \"data_stream\","
                        + "\"INPUT_DATATYPE\": \"PROTO\","
                        + "\"SOURCE_PARQUET_READ_ORDER_STRATEGY\": \\\"EARLIEST_TIME_URL_FIRST,"
                        + "\"SOURCE_PARQUET_FILE_PATHS\": [\"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-05/hr=09/\",  \"gs://p-godata-id-mainstream-bedrock/carbon-offset-transaction-log/dt=2022-02-03/hr=14/\"],"
                        + "\"SOURCE_DETAILS\": [{\"SOURCE_TYPE\": \"BOUNDED\", \"SOURCE_NAME\": \"PARQUET_SOURCE\"}],"
                        + "\"INPUT_SCHEMA_PROTO_CLASS\": \"com.tests.TestMessage\","
                        + "\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\": \"41\"}]");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        assertThrows(JsonSyntaxException.class,
                () -> StreamsFactory.getStreams(configuration, stencilClientOrchestrator, statsDReporterSupplierMock));
    }

    @Test
    public void shouldThrowNullPointerIfStreamConfigIsNotGiven() {
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("");

        assertThrows(NullPointerException.class,
                () -> StreamsFactory.getStreams(configuration, stencilClientOrchestrator, statsDReporterSupplierMock));
    }
}
