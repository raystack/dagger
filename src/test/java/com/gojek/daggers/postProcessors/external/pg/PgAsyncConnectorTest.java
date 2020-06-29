package com.gojek.daggers.postProcessors.external.pg;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.de.stencil.client.StencilClient;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.RowSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

public class PgAsyncConnectorTest {

    ResultFuture<Row> resultFuture;

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
    private PgPool pgClient;
    @Mock
    private io.vertx.sqlclient.Query<RowSet<io.vertx.sqlclient.Row>> executableQuery;

    private WireMockServer wireMockServer;
    private HashMap<String, String> outputMapping;
    private String[] inputColumnNames;
    private Row inputData;
    private Row streamRow;
    private Row outputData;
    private StencilClient stencilClient;
    private ColumnNameManager columnNameManager;
    private boolean telemetryEnabled;
    private long shutDownPeriod;
    private PgSourceConfig pgSourceConfig;
    private String metricId;

    @Before
    public void setUp() {
        initMocks(this);
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn("test-job");
        streamRow = new Row(2);
        inputData = new Row(6);
        outputData = new Row(3);
        streamRow.setField(0, inputData);
        streamRow.setField(1, outputData);
        outputMapping = new HashMap<>();
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "5432", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "customer_id", "select * from public.customers where customer_id = '%s'", false, metricId);
        resultFuture = mock(ResultFuture.class);
        wireMockServer = new WireMockServer(8081);
        wireMockServer.start();
        inputColumnNames = new String[]{"order_id", "event_timestamp", "driver_id", "customer_id", "status", "service_area_id"};
        columnNameManager = new ColumnNameManager(inputColumnNames, new ArrayList<>());
        telemetryEnabled = true;
        shutDownPeriod = 0L;
        stencilClient = mock(StencilClient.class);
        metricId = "metricId-pg-01";
    }

    @After
    public void tearDown() {
        wireMockServer.stop();
    }

    @Test
    public void shouldFetchDescriptorInInvoke() throws Exception {
        inputData.setField(3, "11223344545");

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(stencilClient, times(1)).get("com.gojek.esb.fraud.DriverProfileFlattenLogMessage");
    }

    @Test
    public void shouldCompleteExceptionallyIfDescriptorNotFound() throws Exception {
        inputData.setField(3, "11223344545");

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get(any(String.class))).thenReturn(null);

        pgAsyncConnector.open(configuration);
        try {
            pgAsyncConnector.asyncInvoke(streamRow, resultFuture);
        } catch (Exception e) {
            e.printStackTrace();
        }

        verify(errorReporter, times(1)).reportFatalException(any(DescriptorNotFoundException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(DescriptorNotFoundException.class));
    }


    @Test
    public void shouldNotEnrichOutputWhenQueryVariableIsInvalid() throws Exception {
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "invalid_variable", "select * from public.customers where customer_id = '%s'", false, metricId);
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shoulCompleteExceptionallyWhenQueryVariableFieldIsNullOrRemovedButRequiredInPattern() throws Exception {
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", null, "select * from public.customers where customer_id = '%s'", false, metricId);
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shouldEnrichWhenQueryVariableFieldIsNullOrRemovedButNotRequiredInPattern() throws Exception {
        String query = "select * from public.customers where customer_id = '12345'";
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", null, query, false, metricId);
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);

        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(pgClient.query(query)).thenReturn(executableQuery);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(0)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(0)).markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
        verify(errorReporter, times(0)).reportFatalException(any(InvalidConfigurationException.class));
        verify(pgClient, times(1)).query(any(String.class));
        verify(executableQuery, times(1)).execute(any());
    }

    @Test
    public void shouldNotEnrichOutputWhenQueryPatternIsEmpty() throws Exception {
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "customer_id", "", false, metricId);
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(EMPTY_INPUT);
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shouldGiveErrorWhenQueryPatternIsInvalid() throws Exception {
        inputData.setField(3, "11223344545");
        String invalidQueryPattern = "select * from public.customers where customer_id = %";
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "customer_id", invalidQueryPattern, true, metricId);
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shouldGiveErrorWhenQueryPatternIsIncompatible() throws Exception {
        inputData.setField(3, "11223344545");
        String invalidQueryPattern = "select * from public.customers where customer_id = '%d'";
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "customer_id", invalidQueryPattern, false, metricId);
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shouldNotEnrichOutputForCorrespondingEnrichmentKeyWhenQueryReturnedByClientIsNull() throws Exception {
        inputData.setField(3, "11223344545");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        String query = String.format("select * from public.customers where customer_id = '%s'", "11223344545");

        verify(pgClient, times(1)).query(query);
        verify(errorReporter, times(1)).reportFatalException(any(InvalidConfigurationException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(InvalidConfigurationException.class));
    }

    @Test
    public void shouldGetStencilClientAndEnrichOutputForCorrespondingEnrichmentKeyWhenStencilClientIsNull() throws Exception {
        inputData.setField(3, "11223344545");

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, null);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        String query = String.format("select * from public.customers where customer_id = '%s'", "11223344545");

        verify(stencilClientOrchestrator, times(1)).getStencilClient();
        verify(pgClient, times(1)).query(query);
    }

    @Test
    public void shouldEnrichOutputForCorrespondingEnrichmentKeyWhenQueryReturnedByClientNotNull() throws Exception {
        inputData.setField(3, "11223344545");
        String query = String.format("select * from public.customers where customer_id = '%s'", "11223344545");

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        when(pgClient.query(query)).thenReturn(executableQuery);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);


        verify(pgClient, times(1)).query(query);
        verify(executableQuery, times(1)).execute(any());
    }

    @Test
    public void shouldNotEnrichOutputOnTimeout() throws Exception {
        pgClient = null;
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        pgAsyncConnector.open(configuration);
        pgAsyncConnector.timeout(streamRow, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(TIMEOUTS);
        verify(errorReporter, times(1)).reportNonFatalException(any(TimeoutException.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(streamRow));
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("PG");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        pgAsyncConnector.preProcessBeforeNotifyingSubscriber();

        Assert.assertEquals(metrics, pgAsyncConnector.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        pgAsyncConnector.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(pgAsyncConnector);
    }

    @Test
    public void shouldClosePgClientAndSetItToNullMarkingCloseConnectionEvent() {
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        pgAsyncConnector.close();

        verify(pgClient, times(1)).close();
        Assert.assertNull(pgAsyncConnector.getPgCient());
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
    }

    @Test
    public void shouldReportFatalExceptionAndCompleteExceptionallyWhenFailOnErrorsIsTrue() throws Exception {
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "9200", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "customer_id", "", true, metricId);
        pgClient = null;
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, meterStatsManager, pgClient, telemetryEnabled, errorReporter, shutDownPeriod, stencilClient);
        pgAsyncConnector.open(configuration);
        pgAsyncConnector.timeout(streamRow, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(TIMEOUTS);
        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }


}