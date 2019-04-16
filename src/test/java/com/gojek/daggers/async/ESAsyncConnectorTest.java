package com.gojek.daggers.async;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.gojek.daggers.async.connector.ESAsyncConnector;
import com.gojek.de.stencil.StencilClient;
import com.gojek.esb.customer.CustomerLogMessage;
import com.google.protobuf.Descriptors;
import com.timgroup.statsd.StatsDClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ESAsyncConnectorTest {

    ResultFuture<Row> resultFuture;
    private StatsDClient statsd = mock(StatsDClient.class);
    private ESAsyncConnector esAsyncConnector;
    private WireMockServer wireMockServer;
    private String stubbedResponse = "||{\"_index\":\"customers\",\"_type\":\"customer\",\"_id\":\"2129\",\"_version\":1,\"found\":true,\"_source\":{\"created_at\":\"2016-01-18T08:55:26.16Z\",\"customer_id\":2129,\"email\":\"mtsalis@ymail.com\",\"is_fb_linked\":false,\"name\":\"salis muhammad\",\"phone\":\"mtsalis@ymail.com\",\"type\":\"customer\"}}";


    @Before
    public void setUp() throws Exception {
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
//        esAsyncConnector = new ESAsyncConnector("localhost:8081", 5000, 5000, 5000, statsd, descriptor);
        esAsyncConnector = new ESAsyncConnector(0, new HashMap<>(), mock(StencilClient.class));
        resultFuture = mock(ResultFuture.class);
        esAsyncConnector.open(mock(Configuration.class));
        wireMockServer = new WireMockServer(8081);
        wireMockServer.start();
    }

    @After
    public void tearDown() throws Exception {
        wireMockServer.stop();
    }

    @Test
    public void shouldCallESAsyncInvoke() throws Exception {
        Row input = mock(Row.class);
        when(input.getField(5)).thenReturn("2129");

        esAsyncConnector.asyncInvoke(input, resultFuture);

        Thread.sleep(1000);
        Row result = new Row(1);
        result.setField(0, input);
        verify(resultFuture, times(1)).complete(Collections.singleton(result/*.toString().concat(stubbedResponse)*/));
    }

    @Test
    public void shouldCallESAsyncInvokeNotFoundCustomer() throws Exception {
        Row input = mock(Row.class);
        when(input.getField(5)).thenReturn("11223344545");

        esAsyncConnector.asyncInvoke(input, resultFuture);

        Thread.sleep(1000);
        Row result = new Row(1);
        result.setField(0, input);
        verify(resultFuture, times(1)).complete(Collections.singleton(result/*.toString()*/));
    }

    @Test
    public void shouldNotAddCustomerDetailsForEmptyCustomerId() throws Exception {
        Row input = mock(Row.class);
        when(input.getField(5)).thenReturn("");

        esAsyncConnector.asyncInvoke(input, resultFuture);

        Thread.sleep(1000);
        Row result = new Row(1);
        result.setField(0, input);
        verify(resultFuture).complete(Collections.singleton(result/*.toString()*/));

    }

    @Test
    public void shouldCallESAsyncInvokeCustomer() throws Exception {
        Row input = mock(Row.class);
        when(input.getField(5)).thenReturn("2129");
        ESAsyncConnector esConnector = null;
//        ESAsyncConnector esConnector = new ESAsyncConnector("localhost:9201", 10, 10, 10, statsd);
        esConnector.open(mock(Configuration.class));

        esConnector.asyncInvoke(input, resultFuture);
        Thread.sleep(100);
        Row result = new Row(1);
        result.setField(0, input);
        verify(resultFuture).complete(Collections.singleton(result/*.toString()*/));
    }
}
