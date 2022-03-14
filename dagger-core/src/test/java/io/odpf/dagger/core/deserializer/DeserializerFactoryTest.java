package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.stencil.client.StencilClient;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static io.odpf.dagger.common.serde.DataTypes.JSON;
import static io.odpf.dagger.common.serde.DataTypes.PROTO;
import static io.odpf.dagger.core.source.SourceName.KAFKA;
import static io.odpf.dagger.core.source.SourceName.PARQUET;
import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataTypes.class, SourceName.class})
public class DeserializerFactoryTest {
    @Mock
    private Configuration configuration;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private DataTypes invalidInputSchemaDataType;

    @Mock
    private SourceName invalidSourceName;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setup() {
        initMocks(this);

        invalidInputSchemaDataType = mock(DataTypes.class);
        given(invalidInputSchemaDataType.name()).willReturn("INVALID_INPUT_SCHEMA_DATA_TYPE");
        given(invalidInputSchemaDataType.ordinal()).willReturn(2);
        PowerMockito.mockStatic(DataTypes.class);
        PowerMockito.when(DataTypes.values()).thenReturn(new DataTypes[]{JSON, PROTO, invalidInputSchemaDataType});

        invalidSourceName = mock(SourceName.class);
        given(invalidSourceName.name()).willReturn("INVALID_SOURCE_NAME");
        given(invalidSourceName.ordinal()).willReturn(2);
        PowerMockito.mockStatic(SourceName.class);
        PowerMockito.when(SourceName.values()).thenReturn(new SourceName[]{KAFKA, PARQUET, invalidSourceName});
    }

    @Test
    public void shouldThrowExceptionWhenSourceNameIsNotSupported() {
        IllegalConfigurationException exception = assertThrows(IllegalConfigurationException.class,
                () -> DeserializerFactory.create(invalidSourceName, PROTO, streamConfig, configuration, stencilClientOrchestrator));

        assertEquals("Invalid stream configuration: No suitable deserializer could be constructed for source INVALID_SOURCE_NAME",
                exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenParquetSourceConfiguredWithJSONSchema() {
        IllegalConfigurationException exception = assertThrows(IllegalConfigurationException.class,
                () -> DeserializerFactory.create(PARQUET, JSON, streamConfig, configuration, stencilClientOrchestrator));

        assertEquals("Invalid stream configuration: No suitable Parquet deserializer could be constructed for STREAM_INPUT_DATATYPE with value JSON",
                exception.getMessage());
    }

    @Test
    public void shouldReturnInstanceOfSimpleGroupDeserializerWhenParquetSourceConfiguredWithPROTOSchema() {
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());


        DaggerDeserializer<Row> deserializer = DeserializerFactory.create(PARQUET, PROTO, streamConfig, configuration, stencilClientOrchestrator);

        assertTrue(deserializer instanceof SimpleGroupDeserializer);
    }

    @Test
    public void shouldReturnInstanceOfProtoDeserializerWhenKafkaSourceConfiguredWithPROTOSchema() {
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());


        DaggerDeserializer<Row> deserializer = DeserializerFactory.create(KAFKA, PROTO, streamConfig, configuration, stencilClientOrchestrator);

        assertTrue(deserializer instanceof ProtoDeserializer);
    }

    @Test
    public void shouldReturnInstanceOfJsonDeserializerWhenKafkaSourceConfiguredWithJSONSchema() {
        when(streamConfig.getJsonSchema()).thenReturn("{ \"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"$id\": \"https://example.com/product.schema.json\", \"title\": \"Product\", \"description\": \"A product from Acme's catalog\", \"type\": \"object\", \"properties\": { \"id\": { \"description\": \"The unique identifier for a product\", \"type\": \"string\" }, \"time\": { \"description\": \"event timestamp of the event\", \"type\": \"string\", \"format\" : \"date-time\" } }, \"required\": [ \"id\", \"time\" ] }");

        DaggerDeserializer<Row> deserializer = DeserializerFactory.create(KAFKA, JSON, streamConfig, configuration, null);

        assertTrue(deserializer instanceof JsonDeserializer);
        verify(streamConfig).getJsonEventTimestampFieldName();
    }

    @Test
    public void shouldThrowExceptionWhenKafkaSourceIsConfiguredWithInvalidSchemaDataType() {
        IllegalConfigurationException exception = assertThrows(IllegalConfigurationException.class,
                () -> DeserializerFactory.create(KAFKA, invalidInputSchemaDataType, streamConfig, configuration, stencilClientOrchestrator));

        assertEquals("Invalid stream configuration: No suitable Kafka deserializer could be constructed for STREAM_INPUT_DATATYPE with value INVALID_INPUT_SCHEMA_DATA_TYPE",
                exception.getMessage());
    }
}