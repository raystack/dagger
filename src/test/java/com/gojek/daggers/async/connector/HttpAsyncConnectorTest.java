package com.gojek.daggers.async.connector;

import com.gojek.daggers.Constants;
import com.gojek.daggers.async.metric.ExternalSourceAspects;
import com.gojek.daggers.utils.stats.StatsManager;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.gojek.daggers.Constants.*;
import static com.gojek.daggers.async.metric.ExternalSourceAspects.CLOSE_CONNECTION_ON_HTTP_CLIENT;
import static com.gojek.daggers.async.metric.ExternalSourceAspects.TOTAL_HTTP_CALLS;
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

    @Mock
    private StatsManager statsManager;

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
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.open(flinkConfiguration);
        verify(stencilClient, times(1)).get(outputProto);
    }

    @Test
    public void shouldRegisterStatsManagerInOpen() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.open(flinkConfiguration);
        verify(statsManager, times(1)).register("external.source.http", ExternalSourceAspects.values());
    }

    @Test
    public void shouldCloseHttpClient() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.close();
        verify(httpClient, times(1)).close();
        verify(statsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_HTTP_CLIENT);
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

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.asyncInvoke(inputRow, resultFuture);

        verify(boundRequestBuilder, times(1)).execute(any(HttpResponseHandler.class));
        verify(statsManager, times(1)).markEvent(TOTAL_HTTP_CALLS);
    }

    @Test
    public void shouldThrowExceptionIfBodyFieldKeyNotProvidedInConfig() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Request body key should be passed");

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.asyncInvoke(new Row(1), resultFuture);
    }

    @Test
    public void shouldThrowExceptionIfBodyFieldNotSetInInputRow() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Request body should be selected through SQL Query");

        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        columnNames = new String[]{"abc"};
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.asyncInvoke(new Row(1), resultFuture);
    }

    @Test
    public void shouldThrowExceptionIfEndpointNotProvidedInConfig() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Http Endpoint should be provided");

        columnNames = new String[]{"abc", "request_body"};
        configuration.put(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY, "request_body");
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
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

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
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

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
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

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.asyncInvoke(inputRow, resultFuture);
    }

    @Test
    public void shouldThrowExceptionInTimeoutIfFailOnErrorIsTrue() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Timeout in HTTP Call");

        Row inputRow = new Row(1);
        configuration.put(Constants.EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, true);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.timeout(inputRow, resultFuture);
    }

    @Test
    public void shouldPassTheInputInTimeoutIfFailOnErrorIsFalse() throws Exception {
        Row inputRow = new Row(1);
        inputRow.setField(0, "test");
        configuration.put(Constants.EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.timeout(inputRow, resultFuture);
        Row result = new Row(1);
        result.setField(0, "test");
        verify(resultFuture, times(1)).complete(Collections.singleton(result));
    }
}