package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.source.SourceDetails;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import io.odpf.stencil.client.StencilClient;
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
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        SimpleGroupDeserializerProvider provider = new SimpleGroupDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertTrue(provider.canProvide());
    }

    @Test
    public void shouldNotProvideSimpleGroupDeserializerWhenSourceNameIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        SimpleGroupDeserializerProvider provider = new SimpleGroupDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertFalse(provider.canProvide());
    }

    @Test
    public void shouldNotProvideSimpleGroupDeserializerWhenSchemaTypeIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        SimpleGroupDeserializerProvider provider = new SimpleGroupDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertFalse(provider.canProvide());
    }

    @Test
    public void shouldReturnSimpleGroupDeserializerForSupportedSourceNameAndSchemaType() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET, SourceType.BOUNDED)});
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
