package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.configuration.IllegalConfigurationException;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(PowerMockRunner.class)
public class DeserializerFactoryTest {
    @Mock
    private Configuration configuration;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private ProtoDeserializer protoDeserializer;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    @PrepareForTest(SourceName.class)
    public void shouldThrowExceptionWhenSourceNameIsNotSupported() {
        SourceName sourceName = mock(SourceName.class);
        given(sourceName.name()).willReturn("INVALID_SOURCE");
        given(sourceName.ordinal()).willReturn(2);
        PowerMockito.mockStatic(SourceName.class);
        PowerMockito.when(SourceName.values()).thenReturn(new SourceName[]{KAFKA, PARQUET, sourceName});

        IllegalConfigurationException exception = assertThrows(IllegalConfigurationException.class,
                () -> DeserializerFactory.create(sourceName, PROTO, streamConfig, configuration, stencilClientOrchestrator));

        assertEquals("Invalid stream configuration: No suitable deserializer could be constructed for source of type INVALID_SOURCE",
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
    @PrepareForTest(DataTypes.class)
    public void shouldThrowExceptionWhenKafkaSourceIsConfiguredWithInvalidSchemaDataType() {
        DataTypes inputDataType = mock(DataTypes.class);
        given(inputDataType.name()).willReturn("DUMB_SCHEMA");
        given(inputDataType.ordinal()).willReturn(2);
        PowerMockito.mockStatic(DataTypes.class);
        PowerMockito.when(DataTypes.values()).thenReturn(new DataTypes[]{JSON, PROTO, inputDataType});

        IllegalConfigurationException exception = assertThrows(IllegalConfigurationException.class,
                () -> DeserializerFactory.create(KAFKA, inputDataType, streamConfig, configuration, stencilClientOrchestrator));

        assertEquals("Invalid stream configuration: No suitable Kafka deserializer could be constructed for STREAM_INPUT_DATATYPE with value DUMB_SCHEMA",
                exception.getMessage());
    }
}