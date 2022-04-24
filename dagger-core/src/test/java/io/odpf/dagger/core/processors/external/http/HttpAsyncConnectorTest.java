package io.odpf.dagger.core.processors.external.http;

import io.odpf.stencil.client.StencilClient;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.exception.InvalidHttpVerbException;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.common.OutputMapping;
import io.odpf.dagger.core.processors.external.AsyncConnector;
import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpAsyncConnectorTest {


    private HttpSourceConfig defaultHttpSourceConfig;
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;
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
    @Mock
    private TelemetrySubscriber telemetrySubscriber;
    @Mock
    private ErrorReporter errorReporter;
    @Mock
    private StencilClient stencilClient;
    @Mock
    private DescriptorManager defaultDescriptorManager;
    @Mock
    private SchemaConfig schemaConfig;


    private HashMap<String, OutputMapping> outputMapping;
    private HashMap<String, String> headers;
    private String httpConfigType;
    private Row streamData;
    private ExternalMetricConfig externalMetricConfig;
    private String[] inputProtoClasses;

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
        boolean telemetryEnabled = true;
        long shutDownPeriod = 0L;
        inputProtoClasses = new String[]{"InputProtoMessage"};
        when(schemaConfig.getInputProtoClasses()).thenReturn(inputProtoClasses);
        when(schemaConfig.getColumnNameManager()).thenReturn(new ColumnNameManager(inputColumnNames, outputColumnNames));
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        when(schemaConfig.getOutputProtoClassName()).thenReturn("OutputProtoMessage");
        externalMetricConfig = new ExternalMetricConfig("metricId-http-01", shutDownPeriod, telemetryEnabled);

        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}",
                "customer_id", "", "", "123", "234", false, httpConfigType, "345",
                headers, outputMapping, "metricId_02", false);
    }

    @Test
    public void shouldCloseHttpClient() throws Exception {

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.close();

        verify(httpClient, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
    }

    @Test
    public void shouldMakeHttpClientNullAfterClose() throws Exception {

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.close();

        verify(httpClient, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
        assertNull(httpAsyncConnector.getHttpClient());
    }

    @Test
    public void shouldReturnHttpClient() {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);
        AsyncHttpClient returnedHttpClient = httpAsyncConnector.getHttpClient();
        assertEquals(httpClient, returnedHttpClient);
    }

    @Test
    public void shouldRegisterStatsManagerInOpen() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);

        verify(meterStatsManager, times(1)).register("source_metricId", "HTTP.metricId-http-01", ExternalSourceAspects.values());
    }

    @Test
    public void shouldInitializeDescriptorManagerInOpen() throws Exception {
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        AsyncConnector asyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, null);

        asyncConnector.open(flinkConfiguration);

        verify(stencilClientOrchestrator, times(1)).getStencilClient();
    }

    @Test
    public void shouldFetchDescriptorInInvoke() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        DescriptorManager descriptorManager = new DescriptorManager(stencilClientOrchestrator);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(stencilClient, times(1)).get(httpConfigType);
    }


    @Test
    public void shouldCompleteExceptionallyIfOutputDescriptorNotFound() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(defaultDescriptorManager.getDescriptor(httpConfigType)).thenThrow(new DescriptorNotFoundException());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        verify(errorReporter, times(1)).reportFatalException(any(DescriptorNotFoundException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(DescriptorNotFoundException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenRequestVariableIsInvalid() throws Exception {
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        String invalidRequestVariable = "invalid_variable";
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", invalidRequestVariable, "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        ArgumentCaptor<InvalidConfigurationException> exceptionArgumentCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(exceptionArgumentCaptor.capture());
        assertEquals("Column 'invalid_variable' not found as configured in the 'REQUEST_VARIABLES' variable", exceptionArgumentCaptor.getValue().getMessage());

        ArgumentCaptor<InvalidConfigurationException> futureCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(futureCaptor.capture());

        assertEquals("Column 'invalid_variable' not found as configured in the 'REQUEST_VARIABLES' variable",
                futureCaptor.getValue().getMessage());
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointVariableIsEmptyAndRequiredInPattern() throws Exception {
        String emptyRequestVariable = "";
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", emptyRequestVariable, "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldEnrichWhenEndpointVariableIsEmptyAndNotRequiredInPattern() throws Exception {
        String emptyRequestVariable = "";
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"static\"}", emptyRequestVariable, "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"static\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);


        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(boundRequestBuilder, times(1)).execute(any(HttpResponseHandler.class));
        verify(meterStatsManager, times(1)).markEvent(TOTAL_EXTERNAL_CALLS);
        verify(meterStatsManager, times(0)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(0)).reportFatalException(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointPatternIsInvalid() throws Exception {
        String invalidRequestPattern = "{\"key\": \"%\"}";
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", invalidRequestPattern, "customer_id", "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);
        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldGetDescriptorFromOutputProtoIfTypeNotGiven() throws Exception {
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, null, "345", headers, outputMapping, "metricId_02", false);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        verify(defaultDescriptorManager, times(1)).getDescriptor("InputProtoMessage");
    }

    @Test
    public void shouldGetDescriptorFromTypeIfGiven() throws Exception {
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, "TestBookingLogMessage", "345", headers, outputMapping, "metricId_02", false);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        verify(defaultDescriptorManager, times(1)).getDescriptor("TestBookingLogMessage");
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointPatternIsIncompatible() throws Exception {
        String invalidRequestPattern = "{\"key\": \"%d\"}";
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", invalidRequestPattern, "customer_id", "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);
        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldPerformPostRequestWithCorrectParameters() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(boundRequestBuilder, times(1)).execute(any(HttpResponseHandler.class));
        verify(meterStatsManager, times(1)).markEvent(TOTAL_EXTERNAL_CALLS);
    }

    @Test
    public void shouldAddCustomHeaders() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(boundRequestBuilder, times(1)).addHeader("content-type", "application/json");
    }

    @Test
    public void shouldAddDynamicHeaders() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        HttpSourceConfig dynamicHeaderHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}",
                "customer_id", "{\"X_KEY\": \"%s\"}", "customer_id", "123", "234", false, httpConfigType, "345",
                headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(dynamicHeaderHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        verify(boundRequestBuilder, times(2)).addHeader(anyString(), anyString());
        verify(boundRequestBuilder, times(1)).addHeader("content-type", "application/json");
        verify(boundRequestBuilder, times(1)).addHeader("X_KEY", "123456");
    }

    @Test
    public void shouldNotAddDynamicHeaders() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        HttpSourceConfig dynamicHeaderHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}",
                "customer_id", "{\"X_KEY\": \"%s\"}", "customer_ids", "123", "234", false, httpConfigType, "345",
                headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(dynamicHeaderHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);
        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenHeaderVariableIsInvalid() throws Exception {
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        String invalidHeaderVariable = "invalid_variable";
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "{\"key\": \"%s\"}", invalidHeaderVariable, "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        ArgumentCaptor<InvalidConfigurationException> exceptionArgumentCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(exceptionArgumentCaptor.capture());
        assertEquals("Column 'invalid_variable' not found as configured in the 'HEADER_VARIABLES' variable", exceptionArgumentCaptor.getValue().getMessage());

        ArgumentCaptor<InvalidConfigurationException> futureCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(futureCaptor.capture());

        assertEquals("Column 'invalid_variable' not found as configured in the 'HEADER_VARIABLES' variable",
                futureCaptor.getValue().getMessage());
    }

    @Test
    public void shouldCompleteExceptionallyWhenHeaderPatternIsIncompatible() throws Exception {
        String invalidHeaderPattern = "{\"key\": \"%d\"}";
        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", invalidHeaderPattern, "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);
        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldThrowExceptionInTimeoutIfFailOnErrorIsTrue() throws Exception {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.timeout(streamData, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }

    @Test
    public void shouldReportFatalInTimeoutIfFailOnErrorIsTrue() throws Exception {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.timeout(streamData, resultFuture);

        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldReportNonFatalInTimeoutIfFailOnErrorIsFalse() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.timeout(streamData, resultFuture);

        verify(errorReporter, times(1)).reportNonFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldPassTheInputWithRowSizeCorrespondingToColumnNamesInTimeoutIfFailOnErrorIsFalse() {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.timeout(streamData, resultFuture);
        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
    }

    @Test
    public void shouldThrowExceptionIfUnsupportedHttpVerbProvided() throws Exception {
        when(defaultDescriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "PATCH", "{\"key\": \"%s\"}", "customer_id", "", "", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidHttpVerbException.class));
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("HTTP");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);
        httpAsyncConnector.preProcessBeforeNotifyingSubscriber();

        assertEquals(metrics, httpAsyncConnector.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, defaultDescriptorManager);
        httpAsyncConnector.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(httpAsyncConnector);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfRuntimeContextNotInitialized() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(defaultHttpSourceConfig, externalMetricConfig, schemaConfig, httpClient, null, meterStatsManager, defaultDescriptorManager);
        httpAsyncConnector.open(flinkConfiguration);
    }

}
