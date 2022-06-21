package io.odpf.dagger.core.processors.external.grpc;

import io.odpf.stencil.client.StencilClient;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestGrpcRequest;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.common.OutputMapping;
import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.external.grpc.client.GrpcClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class GrpcAsyncConnectorTest {

    private GrpcSourceConfig grpcSourceConfig;
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;
    @Mock
    private Configuration flinkConfiguration;
    @Mock
    private GrpcClient grpcClient;
    @Mock
    private ResultFuture<Row> resultFuture;
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
    private String grpcConfigType;
    private Row streamData;
    private ExternalMetricConfig externalMetricConfig;
    private String[] inputProtoClasses;
    private String grpcStencilUrl = "http://localhost/feast-proto/latest";

    @Before
    public void setUp() {
        initMocks(this);
        List<String> outputColumnNames = Collections.singletonList("value");
        String[] inputColumnNames = new String[] {"order_id", "customer_id", "driver_id"};
        outputMapping = new HashMap<>();
        grpcConfigType = "type";
        streamData = new Row(2);
        Row inputData = new Row(3);
        inputData.setField(1, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(1));
        boolean telemetryEnabled = true;
        long shutDownPeriod = 0L;
        inputProtoClasses = new String[] {"InputProtoMessage"};
        when(schemaConfig.getInputProtoClasses()).thenReturn(inputProtoClasses);
        when(schemaConfig.getColumnNameManager()).thenReturn(new ColumnNameManager(inputColumnNames, outputColumnNames));
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        when(schemaConfig.getOutputProtoClassName()).thenReturn("OutputProtoMessage");
        externalMetricConfig = new ExternalMetricConfig("metricId-grpc-01", shutDownPeriod, telemetryEnabled);

        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .createGrpcSourceConfig();

    }

    private GrpcSourceConfigBuilder getGrpcSourceConfigBuilder() {
        return new GrpcSourceConfigBuilder()
                .setEndpoint("localhost")
                .setServicePort(8080)
                .setGrpcRequestProtoSchema("test.consumer.TestGrpcRequest")
                .setGrpcResponseProtoSchema("test.meta.GrpcResponse")
                .setGrpcMethodUrl("test.test/TestMethod")
                .setRequestPattern("{'field1': '%s' , 'field2' : 'val2'}")
                .setRequestVariables("customer_id")
                .setStreamTimeout("123")
                .setConnectTimeout("234")
                .setFailOnErrors(true)
                .setGrpcStencilUrl(grpcStencilUrl)
                .setType(grpcConfigType)
                .setRetainResponseType(true)
                .setHeaders(headers)
                .setOutputMapping(new HashMap<>())
                .setMetricId("metricId_02")
                .setCapacity(30);
    }

    @Test
    public void shouldCloseGrpcClient() throws Exception {

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.close();

        verify(grpcClient, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
    }

    @Test
    public void shouldMakeGrpcClientNullAfterClose() throws Exception {

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.close();

        verify(grpcClient, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
        assertNull(grpcAsyncConnector.getGrpcClient());
    }

    @Test
    public void shouldReturnGrpcClient() {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        GrpcClient returnedGrpcClient = grpcAsyncConnector.getGrpcClient();
        assertEquals(grpcClient, returnedGrpcClient);
    }

    @Test
    public void shouldRegisterStatsManagerInOpen() throws Exception {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.open(flinkConfiguration);

        verify(meterStatsManager, times(1)).register("source_metricId", "GRPC.metricId-grpc-01", ExternalSourceAspects.values());
    }

    @Test
    public void shouldInitializeDescriptorManagerInOpen() throws Exception {
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        List<String> grpcSpecificStencilURLs = Arrays.asList(grpcStencilUrl);
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, null);

        grpcAsyncConnector.open(flinkConfiguration);

        verify(stencilClientOrchestrator, times(1)).enrichStencilClient(grpcSpecificStencilURLs);
    }


    @Test
    public void shouldCompleteExceptionallyIfOutputDescriptorNotFound() throws Exception {
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());
        when(descriptorManager.getDescriptor(grpcConfigType)).thenThrow(new DescriptorNotFoundException());

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);
        ArgumentCaptor<DescriptorNotFoundException> reportDescriptorNotFoundCaptor = ArgumentCaptor.forClass(DescriptorNotFoundException.class);
        verify(errorReporter, times(1)).reportFatalException(reportDescriptorNotFoundCaptor.capture());
        assertEquals("descriptor not found", reportDescriptorNotFoundCaptor.getValue().getMessage());
        ArgumentCaptor<DescriptorNotFoundException> descriptorNotFoundExceptionArgumentCaptor = ArgumentCaptor.forClass(DescriptorNotFoundException.class);
        verify(resultFuture, times(1)).completeExceptionally(descriptorNotFoundExceptionArgumentCaptor.capture());
        assertEquals("descriptor not found", descriptorNotFoundExceptionArgumentCaptor.getValue().getMessage());
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointVariableIsInvalid() throws Exception {
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());
        when(descriptorManager.getDescriptor(grpcConfigType)).thenThrow(new DescriptorNotFoundException());

        String invalidRequestVariable = "invalid_variable";
        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .setRequestVariables(invalidRequestVariable)
                .createGrpcSourceConfig();

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        ArgumentCaptor<InvalidConfigurationException> reportInvalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(reportInvalidConfigCaptor.capture());
        assertEquals("Column 'invalid_variable' not found as configured in the 'REQUEST_VARIABLES' variable",
                reportInvalidConfigCaptor.getValue().getMessage());

        ArgumentCaptor<InvalidConfigurationException> invalidConfigurationExceptionArgumentCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigurationExceptionArgumentCaptor.capture());
        assertEquals("Column 'invalid_variable' not found as configured in the 'REQUEST_VARIABLES' variable",
                invalidConfigurationExceptionArgumentCaptor.getValue().getMessage());

    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointVariableIsEmptyAndRequiredInPattern() throws Exception {
        String emptyRequestVariable = "";
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .setRequestVariables(emptyRequestVariable)
                .createGrpcSourceConfig();
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        ArgumentCaptor<InvalidConfigurationException> reportInvalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(reportInvalidConfigCaptor.capture());
        assertEquals("pattern config '{'field1': '%s' , 'field2' : 'val2'}' is incompatible with the variable config ''",
                reportInvalidConfigCaptor.getValue().getMessage());

        ArgumentCaptor<InvalidConfigurationException> invalidConfigurationExceptionArgumentCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigurationExceptionArgumentCaptor.capture());
        assertEquals("pattern config '{'field1': '%s' , 'field2' : 'val2'}' is incompatible with the variable config ''",
                invalidConfigurationExceptionArgumentCaptor.getValue().getMessage());
    }

    @Test
    public void shouldEnrichWhenEndpointVariableIsEmptyAndNotRequiredInPattern() throws Exception {
        String emptyRequestVariable = "";
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());
        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .setRequestPattern("{'field1': 'val1' , 'field2' : 'val2'}")
                .setRequestVariables(emptyRequestVariable)
                .createGrpcSourceConfig();
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);


        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);
        ArgumentCaptor<DynamicMessage> dynamicMessageCaptor = ArgumentCaptor.forClass(DynamicMessage.class);
        TestGrpcRequest expectedRequest = TestGrpcRequest.newBuilder().setField1("val1").setField2("val2").build();

        verify(grpcClient, times(1))
                .asyncUnaryCall(dynamicMessageCaptor.capture(),
                        any(),
                        eq(TestGrpcRequest.getDescriptor()),
                        any());
        assertEquals(expectedRequest, dynamicMessageCaptor.getValue());
        verify(meterStatsManager, times(1)).markEvent(TOTAL_EXTERNAL_CALLS);
        verify(meterStatsManager, times(0)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, never()).reportFatalException(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointPatternIsInvalid() throws Exception {
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());
        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .setRequestPattern("{'field1': 'val1' , 'field2' : '%'}")
                .createGrpcSourceConfig();
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        ArgumentCaptor<InvalidConfigurationException> reportInvalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(reportInvalidConfigCaptor.capture());
        assertEquals("pattern config '{'field1': 'val1' , 'field2' : '%'}' is invalid", reportInvalidConfigCaptor.getValue().getMessage());

        ArgumentCaptor<InvalidConfigurationException> invalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigCaptor.capture());
        assertEquals("pattern config '{'field1': 'val1' , 'field2' : '%'}' is invalid", invalidConfigCaptor.getValue().getMessage());
    }

    @Test
    public void shouldGetDescriptorFromOutputProtoIfTypeNotGiven() throws Exception {
        grpcSourceConfig = getGrpcSourceConfigBuilder().setType(null).createGrpcSourceConfig();
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);
        grpcAsyncConnector.close();
        verify(descriptorManager, times(1)).getDescriptor("InputProtoMessage");
    }

    @Test
    public void shouldGetDescriptorFromTypeIfGiven() throws Exception {
        grpcSourceConfig = new GrpcSourceConfigBuilder()
                .setRequestPattern("{'field1': 'val1' , 'field2' : '%s'}")
                .setRequestVariables("customer_id")
                .setType("test.booking.TestBookingLogMessage")
                .createGrpcSourceConfig();
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor("InputProtoMessage")).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);
        verify(descriptorManager, times(1)).getDescriptor("test.booking.TestBookingLogMessage");
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointPatternIsIncompatible() throws Exception {
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());
        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .setRequestPattern("{'field1': '%d' , 'field2' : 'val2'}")
                .createGrpcSourceConfig();

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        ArgumentCaptor<InvalidConfigurationException> reportInvalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(reportInvalidConfigCaptor.capture());
        assertEquals("pattern config '{'field1': '%d' , 'field2' : 'val2'}' is incompatible with the variable config 'customer_id'",
                reportInvalidConfigCaptor.getValue().getMessage());

        ArgumentCaptor<InvalidConfigurationException> invalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigCaptor.capture());
        assertEquals("pattern config '{'field1': '%d' , 'field2' : 'val2'}' is incompatible with the variable config 'customer_id'",
                invalidConfigCaptor.getValue().getMessage());
    }


    @Test
    public void shouldThrowExceptionInTimeoutIfFailOnErrorIsTrue() throws Exception {
        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .setFailOnErrors(true)
                .createGrpcSourceConfig();
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.timeout(streamData, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }

    @Test
    public void shouldReportFatalInTimeoutIfFailOnErrorIsTrue() {
        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .setFailOnErrors(true)
                .createGrpcSourceConfig();
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.timeout(streamData, resultFuture);

        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldReportNonFatalInTimeoutIfFailOnErrorIsFalse() {
        grpcSourceConfig = getGrpcSourceConfigBuilder()
                .setFailOnErrors(false)
                .createGrpcSourceConfig();
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.timeout(streamData, resultFuture);

        verify(errorReporter, times(1)).reportNonFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldPassTheInputWithRowSizeCorrespondingToColumnNamesInTimeoutIfFailOnErrorIsFalse() {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.timeout(streamData, resultFuture);
        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
    }


    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("GRPC");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        grpcAsyncConnector.preProcessBeforeNotifyingSubscriber();
        assertEquals(metrics, grpcAsyncConnector.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        grpcAsyncConnector.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(grpcAsyncConnector);
    }

    @Test
    public void shouldThrowIfRuntimeContextNotInitialized() {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, null, meterStatsManager, descriptorManager);
        assertThrows(IllegalStateException.class, () -> grpcAsyncConnector.open(flinkConfiguration));
    }

}
