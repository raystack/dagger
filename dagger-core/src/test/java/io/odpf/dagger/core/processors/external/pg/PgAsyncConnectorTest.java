package io.odpf.dagger.core.processors.external.pg;

import io.odpf.stencil.client.StencilClient;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.RowSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
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

public class PgAsyncConnectorTest {

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
    private PgPool pgClient;
    @Mock
    private io.vertx.sqlclient.Query<RowSet<io.vertx.sqlclient.Row>> executableQuery;
    @Mock
    private SchemaConfig schemaConfig;

    private HashMap<String, String> outputMapping = new HashMap<>();
    private Row inputData;
    private Row streamRow;
    private StencilClient stencilClient;
    private PgSourceConfig pgSourceConfig;
    private String metricId;
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
        pgSourceConfig = getPgSourceConfigBuilder()
                .createPgSourceConfig();
        resultFuture = mock(ResultFuture.class);
        String[] inputColumnNames = new String[]{"order_id", "event_timestamp", "driver_id", "customer_id", "status", "service_area_id"};
        boolean telemetryEnabled = true;
        long shutDownPeriod = 0L;

        inputProtoClasses = new String[]{"io.odpf.consumer.TestLogMessage"};
        metricId = "metricId-pg-01";
        externalMetricConfig = new ExternalMetricConfig(metricId, shutDownPeriod, telemetryEnabled);
        stencilClient = mock(StencilClient.class);
        when(schemaConfig.getInputProtoClasses()).thenReturn(inputProtoClasses);
        when(schemaConfig.getColumnNameManager()).thenReturn(new ColumnNameManager(inputColumnNames, new ArrayList<>()));
        when(schemaConfig.getStencilClientOrchestrator()).thenReturn(stencilClientOrchestrator);
        when(schemaConfig.getOutputProtoClassName()).thenReturn("io.odpf.consumer.TestBookingLogMessage");
        when(stencilClient.get(inputProtoClasses[0])).thenReturn(TestBookingLogMessage.getDescriptor());
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
    }

    private PgSourceConfigBuilder getPgSourceConfigBuilder() {
        return new PgSourceConfigBuilder()
                .setHost("localhost")
                .setPort("5432")
                .setUser("user")
                .setPassword("password")
                .setDatabase("db")
                .setType("io.odpf.consumer.TestFlattenLogMessage")
                .setCapacity("30")
                .setStreamTimeout("5000")
                .setOutputMapping(outputMapping)
                .setConnectTimeout("5000")
                .setIdleTimeout("5000")
                .setQueryVariables("customer_id")
                .setQueryPattern("select * from public.customers where customer_id = '%s'")
                .setFailOnErrors(false)
                .setMetricId(metricId)
                .setRetainResponseType(false);
    }

    @Test
    public void shouldFetchDescriptorInInvoke() throws Exception {
        inputData.setField(3, "11223344545");

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(stencilClient, times(1)).get("io.odpf.consumer.TestFlattenLogMessage");
    }

    @Test
    public void shouldCompleteExceptionallyIfDescriptorNotFound() throws Exception {
        inputData.setField(3, "11223344545");

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(stencilClient.get(any(String.class))).thenReturn(null);

        pgAsyncConnector.open(configuration);

         assertThrows(NullPointerException.class,
                () -> pgAsyncConnector.asyncInvoke(streamRow, resultFuture));
        ArgumentCaptor<DescriptorNotFoundException> reportFatalExceptionCaptor = ArgumentCaptor.forClass(DescriptorNotFoundException.class);
        verify(errorReporter, times(1)).reportFatalException(reportFatalExceptionCaptor.capture());
        assertEquals("No Descriptor found for class io.odpf.consumer.TestLogMessage", reportFatalExceptionCaptor.getValue().getMessage());

        ArgumentCaptor<DescriptorNotFoundException> decriptorNotFoundCaptor = ArgumentCaptor.forClass(DescriptorNotFoundException.class);
        verify(resultFuture, times(1)).completeExceptionally(decriptorNotFoundCaptor.capture());
        assertEquals("No Descriptor found for class io.odpf.consumer.TestLogMessage", decriptorNotFoundCaptor.getValue().getMessage());
    }


    @Test
    public void shouldNotEnrichOutputWhenQueryVariableIsInvalid() throws Exception {
        pgSourceConfig = getPgSourceConfigBuilder()
                .setQueryVariables("invalid_variable")
                .createPgSourceConfig();
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        ArgumentCaptor<InvalidConfigurationException> invalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigCaptor.capture());
        assertEquals("Column 'invalid_variable' not found as configured in the 'QUERY_VARIABLES' variable", invalidConfigCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);

        ArgumentCaptor<InvalidConfigurationException> reportExceptionCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(reportExceptionCaptor.capture());
        assertEquals("Column 'invalid_variable' not found as configured in the 'QUERY_VARIABLES' variable", reportExceptionCaptor.getValue().getMessage());
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shoulCompleteExceptionallyWhenQueryVariableFieldIsNullOrRemovedButRequiredInPattern() throws Exception {
        pgSourceConfig = getPgSourceConfigBuilder()
                .setQueryVariables(null)
                .setQueryPattern("select * from public.customers where customer_id = '%s'")
                .createPgSourceConfig();
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);


        ArgumentCaptor<InvalidConfigurationException> invalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigCaptor.capture());
        assertEquals("pattern config 'select * from public.customers where customer_id = '%s'' is incompatible with the variable config 'null'",
                invalidConfigCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);

        ArgumentCaptor<InvalidConfigurationException> reportExceptionCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(reportExceptionCaptor.capture());
        assertEquals("pattern config 'select * from public.customers where customer_id = '%s'' is incompatible with the variable config 'null'",
                reportExceptionCaptor.getValue().getMessage());
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shouldEnrichWhenQueryVariableFieldIsNullOrRemovedButNotRequiredInPattern() throws Exception {
        String query = "select * from public.customers where customer_id = '12345'";
        pgSourceConfig = getPgSourceConfigBuilder()
                .setQueryPattern(query)
                .setQueryVariables(null)
                .createPgSourceConfig();
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);

        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
        when(pgClient.query(query)).thenReturn(executableQuery);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        verify(meterStatsManager, times(0)).markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
        ArgumentCaptor<DescriptorNotFoundException> invalidConfigCaptor = ArgumentCaptor.forClass(DescriptorNotFoundException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigCaptor.capture());
        assertEquals("No Descriptor found for class io.odpf.consumer.TestFlattenLogMessage",
                invalidConfigCaptor.getValue().getMessage());

        ArgumentCaptor<DescriptorNotFoundException> reportExceptionCaptor = ArgumentCaptor.forClass(DescriptorNotFoundException.class);
        verify(errorReporter, times(1)).reportFatalException(reportExceptionCaptor.capture());
        assertEquals("No Descriptor found for class io.odpf.consumer.TestFlattenLogMessage",
                reportExceptionCaptor.getValue().getMessage());

        verify(pgClient, times(1)).query(any(String.class));
        verify(executableQuery, times(1)).execute(any());
    }

    @Test
    public void shouldGiveErrorWhenQueryPatternIsInvalid() throws Exception {
        inputData.setField(3, "11223344545");
        String invalidQueryPattern = "select * from public.customers where customer_id = %";
        pgSourceConfig = getPgSourceConfigBuilder()
                .setQueryPattern(invalidQueryPattern)
                .createPgSourceConfig();
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        ArgumentCaptor<InvalidConfigurationException> invalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigCaptor.capture());
        assertEquals("pattern config 'select * from public.customers where customer_id = %' is invalid",
                invalidConfigCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        ArgumentCaptor<InvalidConfigurationException> reportExceptionCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(reportExceptionCaptor.capture());
        assertEquals("pattern config 'select * from public.customers where customer_id = %' is invalid",
                reportExceptionCaptor.getValue().getMessage());
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shouldGiveErrorWhenQueryPatternIsIncompatible() throws Exception {
        inputData.setField(3, "11223344545");
        String invalidQueryPattern = "select * from public.customers where customer_id = '%d'";

        pgSourceConfig = getPgSourceConfigBuilder()
                .setQueryPattern(invalidQueryPattern)
                .setQueryVariables("customer_id")
                .createPgSourceConfig();
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);

        ArgumentCaptor<InvalidConfigurationException> invalidConfigCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(resultFuture, times(1)).completeExceptionally(invalidConfigCaptor.capture());
        assertEquals("pattern config 'select * from public.customers where customer_id = '%d'' is incompatible with the variable config 'customer_id'",
                invalidConfigCaptor.getValue().getMessage());
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        ArgumentCaptor<InvalidConfigurationException> reportExceptionCaptor = ArgumentCaptor.forClass(InvalidConfigurationException.class);
        verify(errorReporter, times(1)).reportFatalException(reportExceptionCaptor.capture());
        assertEquals("pattern config 'select * from public.customers where customer_id = '%d'' is incompatible with the variable config 'customer_id'",
                reportExceptionCaptor.getValue().getMessage());
        verify(pgClient, never()).query(any(String.class));
    }

    @Test
    public void shouldNotEnrichOutputForCorrespondingEnrichmentKeyWhenQueryReturnedByClientIsNull() throws Exception {
        inputData.setField(3, "11223344545");
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);

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

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
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

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
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
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        pgAsyncConnector.open(configuration);
        pgAsyncConnector.timeout(streamRow, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(TIMEOUTS);
        ArgumentCaptor<TimeoutException> timeoutCaptor = ArgumentCaptor.forClass(TimeoutException.class);
        verify(errorReporter, times(1)).reportNonFatalException(timeoutCaptor.capture());
        assertEquals("Timeout in external source call!", timeoutCaptor.getValue().getMessage());
        verify(resultFuture, times(1)).complete(Collections.singleton(streamRow));
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("PG");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        pgAsyncConnector.preProcessBeforeNotifyingSubscriber();

        assertEquals(metrics, pgAsyncConnector.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        pgAsyncConnector.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(pgAsyncConnector);
    }

    @Test
    public void shouldClosePgClientAndSetItToNullMarkingCloseConnectionEvent() {
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        pgAsyncConnector.close();

        verify(pgClient, times(1)).close();
        assertNull(pgAsyncConnector.getPgClient());
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
    }

    @Test
    public void shouldReportFatalExceptionAndCompleteExceptionallyWhenFailOnErrorsIsTrue() throws Exception {
        pgSourceConfig = getPgSourceConfigBuilder()
                .setFailOnErrors(true)
                .createPgSourceConfig();
        pgClient = null;
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        pgAsyncConnector.open(configuration);
        pgAsyncConnector.timeout(streamRow, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(TIMEOUTS);
        ArgumentCaptor<TimeoutException> timeoutCaptor = ArgumentCaptor.forClass(TimeoutException.class);
        verify(errorReporter, times(1)).reportFatalException(timeoutCaptor.capture());
        assertEquals("Timeout in external source call!", timeoutCaptor.getValue().getMessage());

        ArgumentCaptor<TimeoutException> completeExceptionallyCaptor = ArgumentCaptor.forClass(TimeoutException.class);
        verify(resultFuture, times(1)).completeExceptionally(completeExceptionallyCaptor.capture());
        assertEquals("Timeout in external source call!", completeExceptionallyCaptor.getValue().getMessage());
    }

    @Test
    public void shouldGetDescriptorFromOutputProtoIfTypeNotGiven() throws Exception {
        pgSourceConfig = getPgSourceConfigBuilder()
                .setType(null)
                .createPgSourceConfig();
        inputData.setField(3, "11223344545");

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);
        verify(stencilClient, times(1)).get("io.odpf.consumer.TestBookingLogMessage");
    }

    @Test
    public void shouldGetDescriptorFromTypeIfGiven() throws Exception {
        inputData.setField(3, "11223344545");

        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig, meterStatsManager, pgClient, errorReporter);
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);

        pgAsyncConnector.open(configuration);
        pgAsyncConnector.asyncInvoke(streamRow, resultFuture);
        verify(stencilClient, times(1)).get("io.odpf.consumer.TestFlattenLogMessage");
    }

}
