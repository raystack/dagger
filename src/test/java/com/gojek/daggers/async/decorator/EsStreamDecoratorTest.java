package com.gojek.daggers.async.decorator;

import com.gojek.daggers.async.connector.ESAsyncConnector;
import com.gojek.daggers.async.decorator.async.EsStreamDecorator;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class EsStreamDecoratorTest {

    private StencilClient stencilClient;

    @Before
    public void setUp() throws Exception {
        stencilClient = mock(StencilClient.class);
    }

    @Test
    public void canDecorateES() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "es");
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(configuration, stencilClient, 20, 0);
        assertTrue(esStreamDecorator.canDecorate());
    }

    @Test
    public void shouldNotDecorateOtherThanES() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(configuration, stencilClient, 20, 0);
        assertFalse(esStreamDecorator.canDecorate());
    }

    @Test
    public void shouldReturnProvidedCapacity() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(configuration, stencilClient, 20, 0);
        assertEquals((Integer) 20, esStreamDecorator.getAsyncIOCapacity());
        assertNotEquals((Integer) 25, esStreamDecorator.getAsyncIOCapacity());
    }

    @Test
    public void shouldReturnEsAsyncFunction() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "es");
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(configuration, stencilClient, 20, 0);
        AsyncFunction asyncFunction = esStreamDecorator.getAsyncFunction();
        assertTrue(asyncFunction instanceof ESAsyncConnector);
        assertTrue(asyncFunction instanceof RichAsyncFunction);
    }

    @Test
    public void shouldReturnProvidedStreamTimeout() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "es");
        configuration.put("stream_timeout", "5000");
        EsStreamDecorator esStreamDecorator = new EsStreamDecorator(configuration, stencilClient, 20, 0);
        assertEquals((Integer) 5000, esStreamDecorator.getStreamTimeout());
    }
}