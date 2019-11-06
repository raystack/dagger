package com.gojek.daggers.postProcessors.external.deprecated;

import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import com.gojek.daggers.postProcessors.external.es.EsStreamDecorator;
import com.gojek.de.stencil.StencilClient;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AshikoStreamDecoratorFactoryTest {

    @Test
    public void shouldReturnInputDecorator() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        StencilClient stencilClient = mock(StencilClient.class);
        StreamDecorator streamDecorator = AshikoStreamDecoratorFactory.getStreamDecorator(configuration, 0, stencilClient, 30, 3);
        assertTrue(streamDecorator instanceof InputDecoratorDeprecated);
    }

    @Test
    public void shouldReturnEsStreamDecorator() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "es");
        StencilClient stencilClient = mock(StencilClient.class);
        StreamDecorator streamDecorator = AshikoStreamDecoratorFactory.getStreamDecorator(configuration, 0, stencilClient, 30, 3);
        assertTrue(streamDecorator instanceof EsStreamDecoratorDeprecated);
    }

    @Test
    public void shouldReturnTimestampDecorator() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "timestamp");
        StencilClient stencilClient = mock(StencilClient.class);
        StreamDecorator streamDecorator = AshikoStreamDecoratorFactory.getStreamDecorator(configuration, 0, stencilClient, 30, 3);
        assertTrue(streamDecorator instanceof TimestampDecorator);
    }

}