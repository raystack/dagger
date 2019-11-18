package com.gojek.daggers.postProcessors.external.es;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.ExternalSourceAspects;
import com.gojek.daggers.metrics.StatsManager;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static com.gojek.daggers.metrics.ExternalSourceAspects.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.Silent.class)
public class EsAsyncConnectorTest {

    ResultFuture<Row> resultFuture;

    @Mock
    private Configuration configuration;
    @Mock
    private StatsManager statsManager;

    private WireMockServer wireMockServer;
    private RuntimeContext runtimeContext;
    private Meter meter;
    private RestClient esClient;
    private EsSourceConfig esSourceConfig;
    private HashMap<String, OutputMapping> outputMapping;
    private String[] inputColumnNames;
    private Row inputData;
    private Row streamRow;
    private Row outputData;
    private StencilClient stencilClient;
    private ColumnNameManager columnNameManager;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn("test-job");
        streamRow = new Row(2);
        inputData = new Row(6);
        outputData = new Row(3);
        streamRow.setField(0, inputData);
        streamRow.setField(1, outputData);
        outputMapping = new HashMap<>();
        esSourceConfig = new EsSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "/drivers/driver/%s",
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping);
        esClient = mock(RestClient.class);
        resultFuture = mock(ResultFuture.class);
        wireMockServer = new WireMockServer(8081);
        wireMockServer.start();
        runtimeContext = mock(RuntimeContext.class);
        MetricGroup metricGroup = mock(MetricGroup.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(any())).thenReturn(metricGroup);
        meter = mock(Meter.class);
        when(metricGroup.meter(any(), any())).thenReturn(meter);
        inputColumnNames = new String[]{"order_id", "event_timestamp", "driver_id", "customer_id", "status", "service_area_id"};
        columnNameManager = new ColumnNameManager(inputColumnNames, new ArrayList<>());
        stencilClient = mock(StencilClient.class);
    }

    @After
    public void tearDown() throws Exception {
        wireMockServer.stop();
    }

    @Test
    public void shouldNotEnrichOutputWhenEndpointVariableIsEmpty() throws Exception {
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, stencilClient, columnNameManager, statsManager, esClient);

        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamRow));
        verify(statsManager, times(1)).markEvent(EMPTY_INPUT);
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldNotEnrichOutputWhenEndpointVariableIsInvalid() throws Exception {
        esSourceConfig = new EsSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "/drivers/driver/%s",
                "invalid_variable", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping);
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, stencilClient, columnNameManager, statsManager, esClient);

        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(statsManager, times(1)).markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldNotEnrichOutputWhenEndpointPatternIsEmpty() throws Exception {
        esSourceConfig = new EsSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "",
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping);

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, stencilClient, columnNameManager, statsManager, esClient);
        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamRow));
        verify(statsManager, times(1)).markEvent(EMPTY_INPUT);
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldGiveErrorWhenEndpointPatternIsInvalid() throws Exception {
        inputData.setField(2, "11223344545");
        String invalidEndpointPattern = "/drivers/driver/%";
        esSourceConfig = new EsSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", invalidEndpointPattern,
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping);

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, stencilClient, columnNameManager, statsManager, esClient);
        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(statsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldGiveErrorWhenEndpointPatternIsIncompatible() throws Exception {
        inputData.setField(2, "11223344545");
        String invalidEndpointPattern = "/drivers/driver/%d";
        esSourceConfig = new EsSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", invalidEndpointPattern,
                "driver_id", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", "5000", "5000", "5000", false, outputMapping);

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, stencilClient, columnNameManager, statsManager, esClient);
        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(statsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(esClient, never()).performRequestAsync(any(Request.class), any(EsResponseHandler.class));
    }

    @Test
    public void shouldEnrichOutputForCorrespondingEnrichmentKey() throws Exception {
        inputData.setField(2, "11223344545");

        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, stencilClient, columnNameManager, statsManager, esClient);
        esAsyncConnector.open(configuration);
        esAsyncConnector.asyncInvoke(streamRow, resultFuture);

        Request request = new Request("GET", "/drivers/driver/11223344545");
        verify(statsManager, times(1)).markEvent(TOTAL_ES_CALLS);
        verify(esClient, times(1)).performRequestAsync(eq(request), any(EsResponseHandler.class));
    }

    @Test
    public void shouldNotEnrichOutputOnTimeout() throws Exception {
        esClient = null;
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, stencilClient, columnNameManager, statsManager, esClient);
        esAsyncConnector.open(configuration);
        esAsyncConnector.timeout(streamRow, resultFuture);

        verify(statsManager, times(1)).markEvent(TIMEOUTS);
        verify(resultFuture, times(1)).complete(Collections.singleton(streamRow));
    }
}
