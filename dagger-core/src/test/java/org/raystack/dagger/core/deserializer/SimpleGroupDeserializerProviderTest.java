package org.raystack.dagger.core.deserializer;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.serde.DaggerDeserializer;
import org.raystack.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import org.raystack.dagger.core.source.config.StreamConfig;
import org.raystack.dagger.core.source.config.models.SourceDetails;
import org.raystack.dagger.core.source.config.models.SourceName;
import org.raystack.dagger.core.source.config.models.SourceType;
import org.raystack.dagger.consumer.TestBookingLogMessage;
import org.raystack.stencil.client.StencilClient;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SimpleGroupDeserializerProviderTest {

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private Configuration configuration;

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldBeAbleToProvideSimpleGroupDeserializerWhenSourceNameIsParquetAndSchemaTypeIsPROTO() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        SimpleGroupDeserializerProvider provider = new SimpleGroupDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertTrue(provider.canProvide());
    }

    @Test
    public void shouldNotProvideSimpleGroupDeserializerWhenSourceNameIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        SimpleGroupDeserializerProvider provider = new SimpleGroupDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertFalse(provider.canProvide());
    }

    @Test
    public void shouldNotProvideSimpleGroupDeserializerWhenSchemaTypeIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        SimpleGroupDeserializerProvider provider = new SimpleGroupDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertFalse(provider.canProvide());
    }

    @Test
    public void shouldReturnSimpleGroupDeserializerForSupportedSourceNameAndSchemaType() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        SimpleGroupDeserializerProvider provider = new SimpleGroupDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);
        DaggerDeserializer<Row> daggerDeserializer = provider.getDaggerDeserializer();

        assertTrue(daggerDeserializer instanceof SimpleGroupDeserializer);
    }
}
