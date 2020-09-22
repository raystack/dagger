package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.exception.InvalidHttpVerbException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.SchemaConfig;
import com.gojek.daggers.postProcessors.external.common.DescriptorManager;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.booking.GoFoodBookingLogMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpAsyncConnectorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HttpSourceConfig httpSourceConfig;
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
    private DescriptorManager descriptorManager;
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
        inputProtoClasses = new String[]{"com.gojek.esb.booking.BookingLogMessage"};
        when(schemaConfig.getInputProtoClasses()).thenReturn(inputProtoClasses);
        when(schemaConfig.getColumnNameManager()).thenReturn(new ColumnNameManager(inputColumnNames, outputColumnNames));
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        when(schemaConfig.getOutputProtoClassName()).thenReturn("com.gojek.esb.booking.GoFoodBookingLogMessage");
        externalMetricConfig = new ExternalMetricConfig("metricId-http-01", shutDownPeriod, telemetryEnabled);

        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}",
                "customer_id", "123", "234", false, httpConfigType, "345",
                headers, outputMapping, "metricId_02", false);
    }

    @Test
    public void shouldCloseHttpClient() throws Exception {

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.close();

        verify(httpClient, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
    }

    @Test
    public void shouldMakeHttpClientNullAfterClose() throws Exception {

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.close();

        verify(httpClient, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
        Assert.assertNull(httpAsyncConnector.getHttpClient());
    }

    @Test
    public void shouldReturnHttpClient() {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);
        AsyncHttpClient returnedHttpClient = httpAsyncConnector.getHttpClient();
        Assert.assertEquals(httpClient, returnedHttpClient);
    }

    @Test
    public void shouldRegisterStatsManagerInOpen() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);

        verify(meterStatsManager, times(1)).register("source_metricId", "HTTP.metricId-http-01", ExternalSourceAspects.values());
    }

    @Test
    public void shouldFetchDescriptorInInvoke() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());

        DescriptorManager descriptorManager = new DescriptorManager(stencilClientOrchestrator);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(stencilClient, times(1)).get(httpConfigType);
    }


    @Test
    public void shouldCompleteExceptionallyIfOutputDescriptorNotFound() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(httpConfigType)).thenThrow(new DescriptorNotFoundException());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        try {
            httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }
        verify(errorReporter, times(1)).reportFatalException(any(DescriptorNotFoundException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(DescriptorNotFoundException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointVariableIsInvalid() {
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());
        String invalid_request_variable = "invalid_variable";
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", invalid_request_variable, "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        try {
            httpAsyncConnector.open(flinkConfiguration);
            httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointVariableIsEmptyAndRequiredInPattern() {
        String empty_request_variable = "";
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", empty_request_variable, "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        try {
            httpAsyncConnector.open(flinkConfiguration);
            httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldEnrichWhenEndpointVariableIsEmptyAndNotRequiredInPattern() throws Exception {
        String empty_request_variable = "";
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"static\"}", empty_request_variable, "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

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
    public void shouldCompleteExceptionallyWhenEndpointPatternIsInvalid() {
        String invalidRequestPattern = "{\"key\": \"%\"}";
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", invalidRequestPattern, "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);
        try {
            httpAsyncConnector.open(flinkConfiguration);
            httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldGetDescriptorFromOutputProtoIfTypeNotGiven() throws Exception {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, null, "345", headers, outputMapping, "metricId_02", false);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        verify(descriptorManager, times(1)).getDescriptor("com.gojek.esb.booking.BookingLogMessage");
    }

    @Test
    public void shouldGetDescriptorFromTypeIfGiven() throws Exception {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, "com.gojek.esb.booking.TestBookingLogMessage", "345", headers, outputMapping, "metricId_02", false);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);
        verify(descriptorManager, times(1)).getDescriptor("com.gojek.esb.booking.TestBookingLogMessage");
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointPatternIsIncompatible() throws Exception {
        String invalidRequestPattern = "{\"key\": \"%d\"}";
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", invalidRequestPattern, "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);
        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);
    }

    @Test
    public void shouldPerformPostRequestWithCorrectParameters() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(boundRequestBuilder, times(1)).execute(any(HttpResponseHandler.class));
        verify(meterStatsManager, times(1)).markEvent(TOTAL_EXTERNAL_CALLS);
    }

    @Test
    public void shouldMarkEmptyInputEventAndReturnFromThereWhenRequestBodyIsEmpty() throws Exception {
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(EMPTY_INPUT);
    }

    @Test
    public void shouldAddCustomHeaders() throws Exception {
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody("{\"key\": \"123456\"}")).thenReturn(boundRequestBuilder);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.open(flinkConfiguration);
        httpAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(boundRequestBuilder, times(1)).addHeader("content-type", "application/json");
    }

    @Test
    public void shouldThrowExceptionInTimeoutIfFailOnErrorIsTrue() throws Exception {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.timeout(streamData, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }

    @Test
    public void shouldReportFatalInTimeoutIfFailOnErrorIsTrue() throws Exception {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.timeout(streamData, resultFuture);

        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldReportNonFatalInTimeoutIfFailOnErrorIsFalse() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "customer_id", "123", "234", false, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.timeout(streamData, resultFuture);

        verify(errorReporter, times(1)).reportNonFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldPassTheInputWithRowSizeCorrespondingToColumnNamesInTimeoutIfFailOnErrorIsFalse() {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

        httpAsyncConnector.timeout(streamData, resultFuture);
        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
    }

    @Test
    public void shouldThrowExceptionIfUnsupportedHttpVerbProvided() throws Exception {
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(GoFoodBookingLogMessage.getDescriptor());

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "PATCH", "{\"key\": \"%s\"}", "customer_id", "123", "234", true, httpConfigType, "345", headers, outputMapping, "metricId_02", false);
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);

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

        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);
        httpAsyncConnector.preProcessBeforeNotifyingSubscriber();

        Assert.assertEquals(metrics, httpAsyncConnector.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, errorReporter, meterStatsManager, descriptorManager);
        httpAsyncConnector.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(httpAsyncConnector);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfRuntimeContextNotInitialized() throws Exception {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig, httpClient, null, meterStatsManager, descriptorManager);
        httpAsyncConnector.open(flinkConfiguration);
    }
}
