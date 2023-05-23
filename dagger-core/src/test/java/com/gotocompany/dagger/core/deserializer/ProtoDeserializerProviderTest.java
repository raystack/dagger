package com.gotocompany.dagger.core.deserializer;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.common.serde.DaggerDeserializer;
import com.gotocompany.dagger.common.serde.proto.deserialization.ProtoDeserializer;
import com.gotocompany.dagger.core.source.config.StreamConfig;
import com.gotocompany.dagger.core.source.config.models.SourceDetails;
import com.gotocompany.dagger.core.source.config.models.SourceName;
import com.gotocompany.dagger.core.source.config.models.SourceType;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoDeserializerProviderTest {
    @Mock
    private StreamConfig streamConfig;

    @Mock
    private StencilConfig stencilConfig;

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
    public void shouldBeAbleToProvideProtoDeserializerWhenSourceNameIsKafkaAndSchemaTypeIsPROTO() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        ProtoDeserializerProvider provider = new ProtoDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertTrue(provider.canProvide());
    }

    @Test
    public void shouldBeAbleToProvideProtoDeserializerWhenSourceNameIsKafkaConsumerAndSchemaTypeIsPROTO() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        ProtoDeserializerProvider provider = new ProtoDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertTrue(provider.canProvide());
    }

    @Test
    public void shouldNotProvideProtoDeserializerWhenSourceNameIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.PARQUET_SOURCE, SourceType.BOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");

        ProtoDeserializerProvider provider = new ProtoDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertFalse(provider.canProvide());
    }

    @Test
    public void shouldNotProvideProtoDeserializerWhenSchemaTypeIsUnsupported() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_SOURCE, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("JSON");

        ProtoDeserializerProvider provider = new ProtoDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);

        assertFalse(provider.canProvide());
    }

    @Test
    public void shouldReturnProtoDeserializerForSupportedSourceNameAndSchemaType() {
        when(streamConfig.getSourceDetails()).thenReturn(new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)});
        when(streamConfig.getDataType()).thenReturn("PROTO");
        when(streamConfig.getEventTimestampFieldIndex()).thenReturn("5");
        when(streamConfig.getProtoClass()).thenReturn("com.tests.TestMessage");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilConfig.getCacheAutoRefresh()).thenReturn(false);
        when(stencilClientOrchestrator.createStencilConfig()).thenReturn(stencilConfig);
        when(stencilClient.get("com.tests.TestMessage")).thenReturn(TestBookingLogMessage.getDescriptor());

        ProtoDeserializerProvider provider = new ProtoDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator);
        DaggerDeserializer<Row> daggerDeserializer = provider.getDaggerDeserializer();

        assertTrue(daggerDeserializer instanceof ProtoDeserializer);
    }
}
