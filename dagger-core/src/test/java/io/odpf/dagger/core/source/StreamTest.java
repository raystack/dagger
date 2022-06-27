package io.odpf.dagger.core.source;

import com.timgroup.statsd.StatsDClient;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.metrics.type.statsd.SerializedStatsDClientSupplier;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.source.config.StreamConfig;
import io.odpf.dagger.core.source.config.models.SourceDetails;
import io.odpf.dagger.core.source.config.models.SourceName;
import io.odpf.dagger.core.source.config.models.SourceType;
import io.odpf.dagger.core.source.flinkkafkaconsumer.FlinkKafkaConsumerDaggerSource;
import io.odpf.dagger.core.source.kafka.KafkaDaggerSource;
import io.odpf.dagger.core.source.parquet.ParquetDaggerSource;
import io.odpf.stencil.client.StencilClient;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;


import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamTest {

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private Configuration configuration;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private StreamExecutionEnvironment streamExecutionEnvironment;

    @Mock
    private WatermarkStrategy<Row> watermarkStrategy;

    @Mock
    private DaggerSource<Row> mockDaggerSource;

    private final SerializedStatsDClientSupplier statsDClientSupplierMock = () -> mock(StatsDClient.class);

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldBeAbleToBuildAStreamWithKafkaDaggerSourceAndProtoSchema() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getSchemaTable()).thenReturn("data_stream");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        Stream.Builder builder = new Stream.Builder(streamConfig, configuration, stencilClientOrchestrator, statsDClientSupplierMock);
        Stream stream = builder.build();

        assertTrue(stream.getDaggerSource() instanceof KafkaDaggerSource);
    }

    @Test
    public void shouldBeAbleToBuildAStreamWithFlinkKafkaConsumerDaggerSourceAndProtoSchema() {
        HashMap<String, String> kafkaPropMap = new HashMap<>();
        kafkaPropMap.put("group.id", "dummy-consumer-group");
        kafkaPropMap.put("bootstrap.servers", "localhost:9092");

        Properties properties = new Properties();
        properties.putAll(kafkaPropMap);

        when(streamConfig.getKafkaProps(any())).thenReturn(properties);
        when(streamConfig.getTopicPattern()).thenReturn(Pattern.compile("test-topic"));
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getSchemaTable()).thenReturn("data_stream");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        Stream.Builder builder = new Stream.Builder(streamConfig, configuration, stencilClientOrchestrator, statsDClientSupplierMock);
        Stream stream = builder.build();

        assertTrue(stream.getDaggerSource() instanceof FlinkKafkaConsumerDaggerSource);
    }

    @Test
    public void shouldBeAbleToBuildAStreamWithKafkaDaggerSourceAndJsonSchema() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");
        when(streamConfig.getSchemaTable()).thenReturn("data_stream");
        when(streamConfig.getJsonSchema()).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");

        Stream.Builder builder = new Stream.Builder(streamConfig, configuration, stencilClientOrchestrator, statsDClientSupplierMock);
        Stream stream = builder.build();

        assertTrue(stream.getDaggerSource() instanceof KafkaDaggerSource);
    }

    @Test
    public void shouldBeAbleToBuildAStreamWithFlinkKafkaConsumerDaggerSourceAndJsonSchema() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");
        when(streamConfig.getSchemaTable()).thenReturn("data_stream");
        when(streamConfig.getJsonSchema()).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");

        Stream.Builder builder = new Stream.Builder(streamConfig, configuration, stencilClientOrchestrator, statsDClientSupplierMock);
        Stream stream = builder.build();

        assertTrue(stream.getDaggerSource() instanceof FlinkKafkaConsumerDaggerSource);
    }

    @Test
    public void shouldBeAbleToBuildAStreamWithParquetDaggerSourceAndProtoSchema() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(streamConfig.getSchemaTable()).thenReturn("data_stream");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        Stream.Builder builder = new Stream.Builder(streamConfig, configuration, stencilClientOrchestrator, statsDClientSupplierMock);
        Stream stream = builder.build();

        assertTrue(stream.getDaggerSource() instanceof ParquetDaggerSource);
    }

    @Test
    public void shouldInvokeTheDaggerSourceRegistrationMethodWhenRegisterSourceIsCalled() {
        Stream stream = new Stream(mockDaggerSource, "some-stream");

        stream.registerSource(streamExecutionEnvironment, watermarkStrategy);

        verify(mockDaggerSource, times(1)).register(streamExecutionEnvironment, watermarkStrategy);

    }
}
