package com.gojek.daggers.async.decorator.async;

import com.gojek.daggers.async.connector.HttpAsyncConnector;
import com.gojek.daggers.postprocessor.configs.HttpExternalSourceConfig;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpDecoratorTest {
    @Mock
    private StencilClient stencilClient;

    @Mock
    private HttpExternalSourceConfig httpExternalSourceConfig;

    private String[] inputColumnNames;

    private String type;


    @Before
    public void setUp() {
        initMocks(this);
        when(httpExternalSourceConfig.getStreamTimeout()).thenReturn("3000");
        inputColumnNames = new String[]{"request_body", "order_number"};
        type = "http";
    }

    @Test
    public void canDecorateHttpAync() {
        HttpDecorator httpDecorator = new HttpDecorator(httpExternalSourceConfig, stencilClient, 20, type, inputColumnNames);
        assertTrue(httpDecorator.canDecorate());
    }

    @Test
    public void shouldNotDecorateOtherThanHttpAsync() {
        String type = "es";
        HttpDecorator httpDecorator = new HttpDecorator(httpExternalSourceConfig, stencilClient, 20, type, inputColumnNames);
        assertFalse(httpDecorator.canDecorate());
    }

    @Test
    public void shouldReturnProvidedCapacity() {
        HttpDecorator httpDecorator = new HttpDecorator(httpExternalSourceConfig, stencilClient, 20, type, inputColumnNames);
        assertEquals((Integer) 20, httpDecorator.getAsyncIOCapacity());
        assertNotEquals((Integer) 25, httpDecorator.getAsyncIOCapacity());
    }

    @Test
    public void shouldReturnHttpAsyncFunction() {
        HttpDecorator httpDecorator = new HttpDecorator(httpExternalSourceConfig, stencilClient, 20, type, inputColumnNames);
        AsyncFunction asyncFunction = httpDecorator.getAsyncFunction();
        assertTrue(asyncFunction instanceof HttpAsyncConnector);
        assertTrue(asyncFunction instanceof RichAsyncFunction);
    }

    @Test
    public void shouldReturnProvidedStreamTimeout() {
        HttpDecorator httpDecorator = new HttpDecorator(httpExternalSourceConfig, stencilClient, 20, type, inputColumnNames);
        assertEquals((Integer) 3000, httpDecorator.getStreamTimeout());
    }
}