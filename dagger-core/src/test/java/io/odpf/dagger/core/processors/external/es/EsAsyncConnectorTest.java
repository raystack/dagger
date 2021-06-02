package io.odpf.dagger.core.processors.external.es;

import io.odpf.dagger.consumer.TestBookingLogMessage;
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
import io.odpf.dagger.core.processors.common.OutputMapping;
import com.gojek.de.stencil.client.StencilClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.INVALID_CONFIGURATION;
import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.TIMEOUTS;
import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.TOTAL_EXTERNAL_CALLS;
import static io.odpf.dagger.core.utils.Constants.ES_TYPE;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.Silent.class)
public class EsAsyncConnectorTest {

    private ResultFuture<Row> resultFuture;

    @Mock
    private Configuration configuration;
    @Mock
    private MeterStatsManager meterStatsManager;
    @Mock
    private ErrorReporter errorReporter;
    @Mock
    private TelemetrySubscriber telemetrySubscriber;
    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;
    @Mock
    private SchemaConfig schemaConfig;

    private RestClient esClient;
    private EsSourceConfig esSourceConfig;
    private HashMap<String, OutputMapping> outputMapping;
    private Row inputData;
    private Row streamRow;
    private StencilClient stencilClient;
    private ExternalMetricConfig externalMetricConfig;
    private String[] inputProtoClasses;

    @Before
    public void setUp() {
        initMocks(this);
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn("test-job");
        streamRow = new Row(2);
        inputData = new Row(6);
        Row outputData = new Row(3);
        streamRow.setField(0, inputData);
        streamRow.setField(1, outputData);
        outputMapping = new HashMap<>();
        outputMapping.put("customer_profile", new OutputMapping("$.customer"));
        outputMapping.put("driver_profile", new OutputMapping("$.driver"));
        esSourceConfig = new EsSourceConfig("localhost", "9200", "test_user", "mysecretpassword", "/drivers/driver/%s",
                "driver_id", "TestMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01", false);
        esClient = mock(RestClient.class);
        resultFuture = mock(ResultFuture.class);
        String[] inputColumnNames = new String[]{"order_id", "event_timestamp", "driver_id", "customer_id", "status", "service_area_id"};
        boolean telemetryEnabled = true;
        long shutDownPeriod = 0L;

        inputProtoClasses = new String[]{"InputProtoClass"};
        externalMetricConfig = new ExternalMetricConfig("metricId_01", shutDownPeriod, telemetryEnabled);
        when(schemaConfig.getInputProtoClasses()).thenReturn(inputProtoClasses);
        when(schemaConfig.getColumnNameManager()).thenReturn(new ColumnNameManager(inputColumnNames, new ArrayList<>()));
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        when(schemaConfig.getOutputProtoClassName()).thenReturn("AnotherTestMessage");
        stencilClient = mock(StencilClient.class);
    }

    @Test
    public void shouldNotEnrichOutputWhenEndpointVariableIsEmptyAndRequiredInPattern() throws Exception {
        esSourceConfig = new EsSourceConfig("localhost", "9200", "test_user", "mysecretpassword", "/drivers/driver/%s",
                "", "TestMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01", false);
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);

        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldEnrichOutputWhenEndpointVariableIsEmptyAndNotRequiredInPattern() throws Exception {
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        esSourceConfig = new EsSourceConfig("localhost", "9200", "test_user", "mysecretpassword", "/drivers/",
                "", "TestMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01", false);
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);

        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(esClient, times(1)).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
        verify(resultFuture, times(0)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(0)).markEvent(INVALID_CONFIGURATION);
    }

    @Test
    public void shouldRegisterMetricGroup() throws Exception {
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);

        esAsyncConnector.open(configuration);
        verify(meterStatsManager, times(1)).register("source_metricId", "ES.metricId_01", ExternalSourceAspects.values());
    }

    @Test
    public void shouldFetchDescriptorInInvoke() throws Exception {
        inputData.setField(2, "11223344545");

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(stencilClient, times(1)).get("TestMessage");
    }

    @Test
    public void shouldCompleteExceptionallyIfOutputDescriptorNotFound() throws Exception {
        inputData.setField(2, "11223344545");
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get("TestMessage")).thenReturn(null);

        esAsyncConnector.open(configuration);
        try {
            esAsyncConnector.asyncInvoke(streamRow, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(errorReporter, times(1)).reportFatalException(any(DescriptorNotFoundException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(DescriptorNotFoundException.class));
    }


    @Test
    public void shouldNotEnrichOutputWhenEndpointVariableIsInvalid() throws Exception {
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        esSourceConfig = new EsSourceConfig("localhost", "9200", "test_user", "mysecretpassword", "/drivers/driver/%s",
                "invalid_variable", "TestMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01", false);
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);

        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldGiveErrorWhenEndpointPatternIsInvalid() throws Exception {
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        inputData.setField(2, "11223344545");
        String invalidEndpointPattern = "/drivers/driver/%";
        esSourceConfig = new EsSourceConfig("localhost", "9200", "test_user", "mysecretpassword", invalidEndpointPattern,
                "driver_id", "TestMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01", false);

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldGiveErrorWhenEndpointPatternIsIncompatible() throws Exception {
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        inputData.setField(2, "11223344545");
        String invalidEndpointPattern = "/drivers/driver/%d";
        esSourceConfig = new EsSourceConfig("localhost", "9200", "test_user", "mysecretpassword", invalidEndpointPattern,
                "driver_id", "TestMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01", false);

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldEnrichOutputForCorrespondingEnrichmentKey() throws Exception {
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        inputData.setField(2, "11223344545");

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        Request request = new Request("GET", "/drivers/driver/11223344545");
        verify(meterStatsManager, times(1)).markEvent(TOTAL_EXTERNAL_CALLS);
        verify(esClient, times(1)).performRequestAsync(eq(request), any(EsResponseHandler.class));
    }

    @Test
    public void shouldEnrichOutputWhenUserAndPasswordAreNull() throws Exception {
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        inputData.setField(2, "11223344545");
        esSourceConfig = new EsSourceConfig("localhost", "9200", null, null,
                "/drivers/driver/%s", "driver_id", null, "30",
                "5000", "5000", "5000", "5000", false,
                outputMapping, "metricId_01", false);
        inputData.setField(2, "11223344545");

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        Request request = new Request("GET", "/drivers/driver/11223344545");
        verify(meterStatsManager, times(1)).markEvent(TOTAL_EXTERNAL_CALLS);
        verify(esClient, times(1)).performRequestAsync(eq(request), any(EsResponseHandler.class));
    }

    @Test
    public void shouldNotEnrichOutputOnTimeout() throws Exception {
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        esClient = null;
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        esAsyncConnector.open(configuration);
        esAsyncConnector.timeout(streamRow, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(TIMEOUTS);
        verify(errorReporter, times(1)).reportNonFatalException(any(TimeoutException.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(streamRow));
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add(ES_TYPE);
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        esAsyncConnector.preProcessBeforeNotifyingSubscriber();

        Assert.assertEquals(metrics, esAsyncConnector.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        esAsyncConnector.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(esAsyncConnector);
    }

    @Test
    public void shouldGetDescriptorFromOutputProtoIfTypeNotGiven() throws Exception {
        esSourceConfig = new EsSourceConfig("localhost", "9200", "", "", "/drivers/driver/%s",
                "driver_id", null, "30",
                "5000", "5000", "5000", "5000", false, outputMapping, "metricId_01", false);
        inputData.setField(2, "11223344545");
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        esAsyncConnector.open(configuration);
        try {
            esAsyncConnector.asyncInvoke(streamRow, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(stencilClient, times(1)).get("AnotherTestMessage");
    }

    @Test
    public void shouldGetDescriptorFromTypeIfGiven() throws Exception {
        inputData.setField(2, "11223344545");
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig, esClient, errorReporter, meterStatsManager);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        esAsyncConnector.open(configuration);
        try {
            esAsyncConnector.asyncInvoke(streamRow, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }
        verify(stencilClient, times(1)).get("TestMessage");
    }
}
