package com.gojek.daggers.decorator;

import com.gojek.daggers.async.decorator.*;
import com.gojek.daggers.async.decorator.async.EsStreamDecorator;
import com.gojek.daggers.async.decorator.map.InputDecorator;
import com.gojek.daggers.async.decorator.map.TimestampDecorator;
import com.gojek.de.stencil.StencilClient;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StreamDecoratorFactoryTest {

    @Test
    public void shouldReturnInputDecorator() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        StencilClient stencilClient = mock(StencilClient.class);
        StreamDecorator streamDecorator = StreamDecoratorFactory.getStreamDecorator(configuration, 0, stencilClient, 30, 3);
        assertTrue(streamDecorator instanceof InputDecorator);
    }

    @Test
    public void shouldReturnEsStreamDecorator() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "es");
        StencilClient stencilClient = mock(StencilClient.class);
        StreamDecorator streamDecorator = StreamDecoratorFactory.getStreamDecorator(configuration, 0, stencilClient, 30, 3);
        assertTrue(streamDecorator instanceof EsStreamDecorator);
    }

    @Test
    public void shouldReturnTimestampDecorator() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "timestamp");
        StencilClient stencilClient = mock(StencilClient.class);
        StreamDecorator streamDecorator = StreamDecoratorFactory.getStreamDecorator(configuration, 0, stencilClient, 30, 3);
        assertTrue(streamDecorator instanceof TimestampDecorator);
    }

}