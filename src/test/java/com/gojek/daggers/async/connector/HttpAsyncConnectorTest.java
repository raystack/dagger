package com.gojek.daggers.async.connector;

import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static com.gojek.daggers.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpAsyncConnectorTest {

    private String[] columnNames;
    private Map<String, Object> configuration = new HashMap<>();
    private String outputProto;


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private StencilClient stencilClient;

    @Mock
    private Configuration flinkConfiguration;

    @Mock
    private AsyncHttpClient httpClient;

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private BoundRequestBuilder boundRequestBuilder;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        columnNames = new String[]{"request_body"};
        outputProto = "com.gojek.esb.aggregate.surge.SurgeFactorLogMessage";
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void shouldFetchDecriptorInOpen() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.open(flinkConfiguration);
        verify(stencilClient, times(1)).get(outputProto);
    }

    @Test
    public void shouldCloseHttpClient() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.close();
        verify(httpClient, times(1)).close();
    }

    @Test
    public void shouldPerformPostRequestWithCorrectParameters() throws Exception {
        configuration.put(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY, "http://localhost:8080/test");
        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        HashMap<String, String> headersMap = new HashMap<>();
        headersMap.put("content-type", "application/json");
        configuration.put(EXTERNAL_SOURCE_HTTP_HEADER_KEY, headersMap);

        String requestBody = "{\"key\": \"value\"}";
        Row inputRow = new Row(1);
        inputRow.setField(0, requestBody);
        when(httpClient.preparePost((String) configuration.get(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(requestBody)).thenReturn(boundRequestBuilder);

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.asyncInvoke(inputRow, resultFuture);

        verify(boundRequestBuilder, times(1)).execute(any(HttpResponseHandler.class));
    }

    @Test
    public void shouldThrowExceptionIfBodyFieldKeyNotProvidedInConfig() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Request body key should be passed");

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.asyncInvoke(new Row(1), resultFuture);
    }

    @Test
    public void shouldThrowExceptionIfBodyFieldNotSetInInputRow() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Request body should be selected through SQL Query");

        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        columnNames = new String[]{"abc"};
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.asyncInvoke(new Row(1), resultFuture);
    }

    @Test
    public void shouldThrowExceptionIfEndpointNotProvidedInConfig() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Http Endpoint should be provided");

        columnNames = new String[]{"abc", "request_body"};
        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.asyncInvoke(new Row(1), resultFuture);
    }

    @Test
    public void shouldAddCustomHeaders() throws Exception {
        configuration.put(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY, "http://localhost:8080/test");
        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        HashMap<String, String> headersMap = new HashMap<>();
        headersMap.put("content-type", "application/json");
        configuration.put(EXTERNAL_SOURCE_HTTP_HEADER_KEY, headersMap);

        String requestBody = "{\"key\": \"value\"}";
        Row inputRow = new Row(1);
        inputRow.setField(0, requestBody);
        when(httpClient.preparePost((String) configuration.get(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(requestBody)).thenReturn(boundRequestBuilder);

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.asyncInvoke(inputRow, resultFuture);

        verify(boundRequestBuilder, times(1)).addHeader("content-type", "application/json");
    }

    @Test
    public void shouldNotAddHeadersIfNotProvidedInConfig() throws Exception {
        configuration.put(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY, "http://localhost:8080/test");
        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");

        String requestBody = "{\"key\": \"value\"}";
        Row inputRow = new Row(1);
        inputRow.setField(0, requestBody);
        when(httpClient.preparePost((String) configuration.get(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(requestBody)).thenReturn(boundRequestBuilder);

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.asyncInvoke(inputRow, resultFuture);
        verify(boundRequestBuilder, times(0)).addHeader("content-type", "application/json");
    }

    @Test
    public void shouldThrowExceptionIfMapConfigNotProvidedForHeaders() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Config for header should be a map");

        configuration.put(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY, "http://localhost:8080/test");
        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        configuration.put(EXTERNAL_SOURCE_HTTP_HEADER_KEY, "application/json");

        String requestBody = "{\"key\": \"value\"}";
        Row inputRow = new Row(1);
        inputRow.setField(0, requestBody);
        when(httpClient.preparePost((String) configuration.get(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(requestBody)).thenReturn(boundRequestBuilder);

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient);
        httpAsyncConnector.asyncInvoke(inputRow, resultFuture);
    }
}