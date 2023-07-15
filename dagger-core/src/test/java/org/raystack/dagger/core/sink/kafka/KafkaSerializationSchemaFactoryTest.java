package org.raystack.dagger.core.sink.kafka;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.core.sink.kafka.builder.KafkaJsonSerializerBuilder;
import org.raystack.dagger.core.sink.kafka.builder.KafkaProtoSerializerBuilder;
import org.raystack.dagger.core.utils.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KafkaSerializationSchemaFactoryTest {
    @Mock
    private Configuration configuration;
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateJsonSerializerForJsonSinkDataTypes() {
        when(configuration.getString(Constants.SINK_KAFKA_DATA_TYPE, "PROTO")).thenReturn("JSON");
        KafkaSerializerBuilder serializationSchema = KafkaSerializationSchemaFactory
                .getSerializationSchema(configuration, stencilClientOrchestrator, new String[]{"test-col"});

        Assert.assertTrue(serializationSchema instanceof KafkaJsonSerializerBuilder);
    }

    @Test
    public void shouldCreateProtoSerializerForProtoSinkDataTypes() {
        when(configuration.getString(Constants.SINK_KAFKA_DATA_TYPE, "PROTO")).thenReturn("PROTO");
        KafkaSerializerBuilder serializationSchema = KafkaSerializationSchemaFactory
                .getSerializationSchema(configuration, stencilClientOrchestrator, new String[]{"test-col"});

        Assert.assertTrue(serializationSchema instanceof KafkaProtoSerializerBuilder);
    }
}
