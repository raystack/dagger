package com.gojek.daggers.async.connector;

import com.gojek.daggers.async.metric.ExternalSourceAspects;
import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.daggers.utils.stats.StatsManager;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.async.metric.ExternalSourceAspects.CLOSE_CONNECTION_ON_HTTP_CLIENT;
import static com.gojek.daggers.async.metric.ExternalSourceAspects.TOTAL_HTTP_CALLS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpAsyncConnectorTest {

    private String[] columnNames;
    private String outputProto;


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    public HttpExternalSourceConfig httpExternalSourceConfig;

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

    @Test
    public void shouldFetchDecriptorInOpen() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.open(flinkConfiguration);
        verify(stencilClient, times(1)).get(outputProto);
    }

    @Test
    public void shouldRegisterStatsManagerInOpen() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.open(flinkConfiguration);
        verify(statsManager, times(1)).register("external.source.http", ExternalSourceAspects.values());
    }

    @Test
    public void shouldCloseHttpClient() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.close();
        verify(httpClient, times(1)).close();
        verify(statsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_HTTP_CLIENT);
    }

    @Test
    public void shouldPerformPostRequestWithCorrectParameters() throws Exception {
        when(httpExternalSourceConfig.getEndpoint()).thenReturn("http://localhost:8080/test");
        when(httpExternalSourceConfig.getBodyField()).thenReturn("request_body");

        HashMap<String, String> headersMap = new HashMap<>();
        headersMap.put("content-type", "application/json");

        when(httpExternalSourceConfig.getHeaders()).thenReturn(headersMap);

        String requestBody = "{\"key\": \"value\"}";
        Row inputRow = new Row(1);
        inputRow.setField(0, requestBody);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(requestBody)).thenReturn(boundRequestBuilder);

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.asyncInvoke(inputRow, resultFuture);

        verify(boundRequestBuilder, times(1)).execute(any(HttpResponseHandler.class));
        verify(statsManager, times(1)).markEvent(TOTAL_HTTP_CALLS);
    }

    @Test
    public void shouldThrowExceptionIfBodyFieldNotSetInInputRow() throws Exception {
        when(httpExternalSourceConfig.getBodyField()).thenReturn("request_body");
        columnNames = new String[]{"abc"};
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto, httpClient, statsManager);
        try {
            httpAsyncConnector.asyncInvoke(new Row(1), resultFuture);
        } catch (Exception e) {

        }
        verify(resultFuture, times(1)).completeExceptionally(any(IllegalArgumentException.class));
    }


    @Test
    public void shouldAddCustomHeaders() throws Exception {
        when(httpExternalSourceConfig.getEndpoint()).thenReturn("http://localhost:8080/test");
        when(httpExternalSourceConfig.getBodyField()).thenReturn("request_body");

        HashMap<String, String> headersMap = new HashMap<>();
        headersMap.put("content-type", "application/json");

        when(httpExternalSourceConfig.getHeaders()).thenReturn(headersMap);

        String requestBody = "{\"key\": \"value\"}";
        Row inputRow = new Row(1);
        inputRow.setField(0, requestBody);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(requestBody)).thenReturn(boundRequestBuilder);

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.asyncInvoke(inputRow, resultFuture);

        verify(boundRequestBuilder, times(1)).addHeader("content-type", "application/json");
    }


    @Test
    public void shouldThrowExceptionInTimeoutIfFailOnErrorIsTrue() throws Exception {
        Row inputRow = new Row(1);
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(true);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.timeout(inputRow, resultFuture);
        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }

    @Test
    public void shouldPassTheInputInTimeoutIfFailOnErrorIsFalse() throws Exception {
        Row inputRow = new Row(1);
        inputRow.setField(0, "test");
        when(httpExternalSourceConfig.isFailOnErrors()).thenReturn(false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto, httpClient, statsManager);
        httpAsyncConnector.timeout(inputRow, resultFuture);
        Row result = new Row(1);
        result.setField(0, "test");
        verify(resultFuture, times(1)).complete(Collections.singleton(result));
    }
}