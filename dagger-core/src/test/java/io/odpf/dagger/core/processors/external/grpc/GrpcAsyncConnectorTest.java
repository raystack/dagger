package io.odpf.dagger.core.processors.external.grpc;

import com.gojek.de.stencil.client.StencilClient;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestGrpcRequest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.external.SchemaConfig;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.common.OutputMapping;
import io.odpf.dagger.core.processors.external.grpc.client.GrpcClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.*;
import java.util.concurrent.TimeoutException;

import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class GrpcAsyncConnectorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
    private String grpcStencilUrl;

    @Before
    public void setUp() {
        initMocks(this);
        List<String> outputColumnNames = Collections.singletonList("value");
        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        outputMapping = new HashMap<>();
        grpcConfigType = "type";
        streamData = new Row(2);
        Row inputData = new Row(3);
        inputData.setField(1, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(1));
        boolean telemetryEnabled = true;
        long shutDownPeriod = 0L;
        grpcStencilUrl = "http://localhost/feast-proto/latest";
        inputProtoClasses = new String[]{"InputProtoMessage"};
        when(schemaConfig.getInputProtoClasses()).thenReturn(inputProtoClasses);
        when(schemaConfig.getColumnNameManager()).thenReturn(new ColumnNameManager(inputColumnNames, outputColumnNames));
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        when(schemaConfig.getOutputProtoClassName()).thenReturn("OutputProtoMessage");
        externalMetricConfig = new ExternalMetricConfig("metricId-grpc-01", shutDownPeriod, telemetryEnabled);

        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': '%s' , 'field2' : 'val2'}",
                "customer_id", "123", "234", true, grpcStencilUrl, grpcConfigType, true,
                headers, outputMapping, "metricId_02", 30);

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
        Assert.assertNull(grpcAsyncConnector.getGrpcClient());
    }

    @Test
    public void shouldReturnGrpcClient() {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        GrpcClient returnedGrpcClient = grpcAsyncConnector.getGrpcClient();
        Assert.assertEquals(grpcClient, returnedGrpcClient);
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
        String[] grpcStencils = {grpcStencilUrl};
        List<String> grpcSpecificStencilURLs = Arrays.asList(grpcStencils);
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
        try {
            grpcAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }
        verify(errorReporter, times(1)).reportFatalException(any(DescriptorNotFoundException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(DescriptorNotFoundException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointVariableIsInvalid() {
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());
        when(descriptorManager.getDescriptor(grpcConfigType)).thenThrow(new DescriptorNotFoundException());

        String invalidRequestVariable = "invalid_variable";
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': '%s' , 'field2' : 'val2'}",
                invalidRequestVariable, "123", "234", true, grpcStencilUrl, grpcConfigType, true,
                headers, outputMapping, "metricId_02", 30);

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        try {
            grpcAsyncConnector.open(flinkConfiguration);
            grpcAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointVariableIsEmptyAndRequiredInPattern() {
        String emptyRequestVariable = "";
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': '%s' , 'field2' : 'val2'}",
                emptyRequestVariable, "123", "234", true, grpcStencilUrl, grpcConfigType, true,
                headers, outputMapping, "metricId_02", 30);

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        try {
            grpcAsyncConnector.open(flinkConfiguration);
            grpcAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldEnrichWhenEndpointVariableIsEmptyAndNotRequiredInPattern() throws Exception {
        String emptyRequestVariable = "";
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());


        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': 'val1' , 'field2' : 'val2'}",
                emptyRequestVariable, "123", "234", true, grpcStencilUrl, grpcConfigType, true,
                headers, outputMapping, "metricId_02", 30);

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);


        grpcAsyncConnector.open(flinkConfiguration);
        grpcAsyncConnector.asyncInvoke(streamData, resultFuture);

        verify(grpcClient, times(1)).asyncUnaryCall(any(), any(), any(), any());
        verify(meterStatsManager, times(1)).markEvent(TOTAL_EXTERNAL_CALLS);
        verify(meterStatsManager, times(0)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(0)).reportFatalException(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldCompleteExceptionallyWhenEndpointPatternIsInvalid() {
        when(descriptorManager.getDescriptor(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(descriptorManager.getDescriptor(grpcSourceConfig.getGrpcRequestProtoSchema())).thenReturn(TestGrpcRequest.getDescriptor());


        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': 'val1' , 'field2' : '%'}",
                "customer_id", "123", "234", true, grpcStencilUrl, grpcConfigType, true,
                headers, outputMapping, "metricId_02", 30);

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        try {
            grpcAsyncConnector.open(flinkConfiguration);
            grpcAsyncConnector.asyncInvoke(streamData, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldGetDescriptorFromOutputProtoIfTypeNotGiven() throws Exception {
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': 'val1' , 'field2' : '%'}",
                "customer_id", "123", "234", true, grpcStencilUrl, null, true,
                headers, outputMapping, "metricId_02", 30);

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
        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': 'val1' , 'field2' : '%s'}",
                "customer_id", "123", "234", true, grpcStencilUrl, "test.booking.TestBookingLogMessage", false,
                headers, outputMapping, "metricId_02", 30);

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


        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': '%d' , 'field2' : 'val2'}",
                "customer_id", "123", "234", true, grpcStencilUrl, grpcConfigType, true,
                headers, outputMapping, "metricId_02", 30);

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        try {
            grpcAsyncConnector.open(flinkConfiguration);
            grpcAsyncConnector.asyncInvoke(streamData, resultFuture);

        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }


    @Test
    public void shouldThrowExceptionInTimeoutIfFailOnErrorIsTrue() throws Exception {


        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': 'val1' , 'field2' : '%'}",
                "customer_id", "123", "234", true, grpcStencilUrl, null, true,
                headers, outputMapping, "metricId_02", 30);


        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.timeout(streamData, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }

    @Test
    public void shouldReportFatalInTimeoutIfFailOnErrorIsTrue() {

        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': 'val1' , 'field2' : '%'}",
                "customer_id", "123", "234", true, grpcStencilUrl, null, true,
                headers, outputMapping, "metricId_02", 30);

        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);

        grpcAsyncConnector.timeout(streamData, resultFuture);

        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldReportNonFatalInTimeoutIfFailOnErrorIsFalse() {

        grpcSourceConfig = new GrpcSourceConfig("localhost", 8080, "test.consumer.TestGrpcRequest", "test.meta.GrpcResponse", "test.test/TestMethod", "{'field1': 'val1' , 'field2' : '%'}",
                "customer_id", "123", "234", false, grpcStencilUrl, null, true,
                headers, outputMapping, "metricId_02", 30);

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

        Assert.assertEquals(metrics, grpcAsyncConnector.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, errorReporter, meterStatsManager, descriptorManager);
        grpcAsyncConnector.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(grpcAsyncConnector);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfRuntimeContextNotInitialized() throws Exception {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig, grpcClient, null, meterStatsManager, descriptorManager);
        grpcAsyncConnector.open(flinkConfiguration);
    }

}
