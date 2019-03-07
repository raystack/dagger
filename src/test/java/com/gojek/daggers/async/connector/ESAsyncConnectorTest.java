package com.gojek.daggers.async.connector;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.recording.SnapshotRecordResult;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.timgroup.statsd.StatsDClient;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ESAsyncConnectorTest {

    private StatsDClient statsd = mock(StatsDClient.class);
    private ESAsyncConnector esAsyncConnector;
    ResultFuture<Tuple2<String, String>> resultFuture;

    private WireMockServer wireMockServer;
    private String stubbedResponse = "||{\"_index\":\"customers\",\"_type\":\"customer\",\"_id\":\"2129\",\"_version\":1,\"found\":true,\"_source\":{\"created_at\":\"2016-01-18T08:55:26.16Z\",\"customer_id\":2129,\"email\":\"mtsalis@ymail.com\",\"is_fb_linked\":false,\"name\":\"salis muhammad\",\"phone\":\"mtsalis@ymail.com\",\"type\":\"customer\"}}";


    @Before
    public void setUp() throws Exception {
        esAsyncConnector = new ESAsyncConnector("localhost:8081", 5000, 5000, 5000, statsd);
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
        verify(resultFuture, times(1)).complete(Collections.singleton(new Tuple2<>("2129", input.toString().concat(stubbedResponse))));
    }

    @Test
    public void shouldCallESAsyncInvokeNotFoundCustomer() throws Exception {
        Row input = mock(Row.class);
        when(input.getField(5)).thenReturn("11223344545");

        esAsyncConnector.asyncInvoke(input, resultFuture);

        Thread.sleep(1000);
        verify(resultFuture, times(1)).complete(Collections.singleton(new Tuple2<>("11223344545", input.toString())));
    }

    @Test
    public void shouldNotAddCustomerDetailsForEmptyCustomerId() throws Exception {
        Row input = mock(Row.class);
        when(input.getField(5)).thenReturn("");

        esAsyncConnector.asyncInvoke(input, resultFuture);

        Thread.sleep(1000);
        verify(resultFuture).complete(Collections.singleton(new Tuple2<>("", input.toString())));

    }

    @Test
    public void shouldCallESAsyncInvokeCustomer() throws Exception {
        Row input = mock(Row.class);
        when(input.getField(5)).thenReturn("2129");
        ESAsyncConnector esConnector = new ESAsyncConnector("localhost:9201", 10, 10, 10, statsd);
        esConnector.open(mock(Configuration.class));

        esConnector.asyncInvoke(input, resultFuture);
        Thread.sleep(100);
        verify(resultFuture).complete(Collections.singleton(new Tuple2<>("2129", input.toString())));
    }
}
