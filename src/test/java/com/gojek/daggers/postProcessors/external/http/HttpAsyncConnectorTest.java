package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
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
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpAsyncConnectorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HttpSourceConfig httpSourceConfig;
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
    private MeterStatsManager meterStatsManager;
    private ColumnNameManager columnNameManager;
    private HashMap<String, OutputMapping> outputMapping;
    private HashMap<String, String> headers;
    private String httpConfigType;
    private Row streamData;

    @Before
    public void setUp() {
        initMocks(this);
        List<String> outputColumnNames = Collections.singletonList("value");
        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        outputMapping = new HashMap<>();
        headers = new HashMap<>();
        headers.put("content-type", "application/json");
        httpConfigType = "type";
        streamData = new Row(2);
        Row inputData = new Row(3);
        inputData.setField(1, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(1));
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping);
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
    }

    @Test
    public void shouldCloseHttpClient() throws Exception {

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        httpAsyncConnector.close();

        verify(httpClient, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_HTTP_CLIENT);
    }

    @Test
    public void shouldFetchDescriptorInOpen() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        httpAsyncConnector.open(flinkConfiguration);

        verify(stencilClient, times(1)).get(httpConfigType);
    }

    @Test
    public void shouldRegisterStatsManagerInOpen() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        httpAsyncConnector.open(flinkConfiguration);

        verify(meterStatsManager, times(1)).register("external.source.http", ExternalSourceAspects.values());
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointVariableIsInvalid() {
        String invalid_request_variable = "invalid_variable";
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", invalid_request_variable, "123", "234", false, httpConfigType, "345", headers, outputMapping);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        try {
            httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointPatternIsInvalid() {
        String invalidRequestPattern = "{\"key\": \"%\"}";
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", invalidRequestPattern, "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);
        try {
            httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointPatternIsIncompatible() {
        String invalidRequestPattern = "{\"key\": \"%d\"}";
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", invalidRequestPattern, "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);
        try {
            httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldPerformPostRequestWithCorrectParameters() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(boundRequestBuilder, times(1)).execute(any(HttpResponseHandler.class));
        verify(meterStatsManager, times(1)).markEvent(TOTAL_HTTP_CALLS);
    }

    @Test
    public void shouldMarkEmptyInputEventAndReturnFromThereWhenRequestBodyIsEmpty() throws Exception {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(EMPTY_INPUT);
    }

    @Test
    public void shouldThrowExceptionIfBodyFieldNotSetInInputRow() {
//        when(httpSourceConfig.getBodyPattern()).thenReturn("request_body");
//        columnNames = new String[]{"abc"};
//        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(columnNames, httpSourceConfig, stencilClient, httpClient, meterStatsManager);
//        try {
//            httpAsyncConnector.asyncInvoke(new Row(1), resultFuture);
//        } catch (Exception e) {
//
//        }
//        verify(resultFuture, times(1)).completeExceptionally(any(IllegalArgumentException.class));
    }


    @Test
    public void shouldAddCustomHeaders() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(boundRequestBuilder, times(1)).addHeader("content-type", "application/json");
    }


    @Test
    public void shouldThrowExceptionInTimeoutIfFailOnErrorIsTrue() throws Exception {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        httpAsyncConnector.timeout(streamData, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }

    @Test
    public void shouldPassTheInputWithRowSizeCorrespondingToColumnNamesInTimeoutIfFailOnErrorIsFalse() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClient, httpClient, meterStatsManager, columnNameManager);

        httpAsyncConnector.timeout(streamData, resultFuture);
        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
    }
}