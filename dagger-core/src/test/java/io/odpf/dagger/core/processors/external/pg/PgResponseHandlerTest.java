package io.odpf.dagger.core.processors.external.pg;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestSurgeFactorLogMessage;
import io.odpf.dagger.core.exception.HttpFailureException;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.PostResponseTelemetry;
import io.odpf.dagger.core.processors.common.RowManager;
import io.vertx.core.AsyncResult;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
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

import static io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class PgResponseHandlerTest {

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private MeterStatsManager meterStatsManager;

    @Mock
    private PgSourceConfig pgSourceConfig;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private AsyncResult<RowSet<io.vertx.sqlclient.Row>> event;

    @Mock
    private RowSet<io.vertx.sqlclient.Row> resultRowSet;

    @Mock
    private io.vertx.sqlclient.Row resultRow;

    @Mock
    private RowIterator<io.vertx.sqlclient.Row> rowSetIterator;

    @Mock
    private io.vertx.sqlclient.Row row;

    private Descriptors.Descriptor descriptor;
    private List<String> outputColumnNames;
    private String[] inputColumnNames;
    private HashMap<String, String> outputMapping = new HashMap<>();
    private Row streamData;
    private RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private Row inputData;
    private String metricId;

    @Before
    public void setup() {
        initMocks(this);
        descriptor = TestBookingLogMessage.getDescriptor();
        inputColumnNames = new String[0];
        outputColumnNames = new ArrayList<>();
        outputColumnNames.add("customer_url");
        outputColumnNames.add("activity_source");
        streamData = new Row(2);
        inputData = new Row(3);
        inputData.setField(1, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(2));
        rowManager = new RowManager(streamData);
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        pgSourceConfig = getPgSourceConfigBuilder().createPgSourceConfig();
    }

    private PgSourceConfigBuilder getPgSourceConfigBuilder() {
        return new PgSourceConfigBuilder()
                .setHost("localhost")
                .setPort("5432")
                .setUser("user")
                .setPassword("password")
                .setDatabase("db")
                .setType("ProtoClass")
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
    public void shouldGoToSuccessHandlerAndMarkSuccessResponseIfEventSucceedsAndResultSetHasOnlyOneRow() {
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(event.succeeded()).thenReturn(true);
        when(event.result()).thenReturn(resultRowSet);
        when(resultRowSet.size()).thenReturn(1);

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldGoToSuccessHandlerButReturnWithMarkingInvalidConfigIfEventSucceedsAndResultSetHasMultipleRow() {
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(event.succeeded()).thenReturn(true);
        when(event.result()).thenReturn(resultRowSet);
        when(resultRowSet.size()).thenReturn(2);

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(meterStatsManager, never()).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldGoToSuccessHandlerButCompleteWithNonFatalErrorWhenFailOnErrorIsFalseAndIfEventSucceedsAndResultSetHasMultipleRow() {
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(event.succeeded()).thenReturn(true);
        when(event.result()).thenReturn(resultRowSet);
        when(resultRowSet.size()).thenReturn(2);

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(meterStatsManager, never()).updateHistogram(any(Aspects.class), any(Long.class));
        verify(errorReporter, times(1)).reportNonFatalException(any(Exception.class));
    }

    @Test
    public void shouldGoToSuccessHandlerButCompleteExceptionallyWithFatalErrorWhenFailOnErrorIsTrueAndIfEventSucceedsAndResultSetHasMultipleRow() {
        pgSourceConfig = getPgSourceConfigBuilder()
                .setFailOnErrors(true)
                .createPgSourceConfig();
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(event.succeeded()).thenReturn(true);
        when(event.result()).thenReturn(resultRowSet);
        when(resultRowSet.size()).thenReturn(2);

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        verify(resultFuture, times(1)).completeExceptionally(any(Exception.class));
        verify(meterStatsManager, times(1)).markEvent(INVALID_CONFIGURATION);
        verify(meterStatsManager, never()).updateHistogram(any(Aspects.class), any(Long.class));
        verify(errorReporter, times(1)).reportFatalException(any(Exception.class));
    }

    @Test
    public void shouldGoToFailureHandlerIfEventFails() {
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(event.succeeded()).thenReturn(false);
        when(event.cause()).thenReturn(new Exception("failure message!"));

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldReportFatalExceptionAndCompleteExceptionallyWhenEventComesToFailureHandleAndFailOnErrorsIsTrue() {
        pgSourceConfig = getPgSourceConfigBuilder()
                .setFailOnErrors(true)
                .createPgSourceConfig();
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(event.succeeded()).thenReturn(false);
        when(event.cause()).thenReturn(new Exception("failure message!"));

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        ArgumentCaptor<HttpFailureException> fatalExcepCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportFatalException(fatalExcepCaptor.capture());
        assertEquals("PgResponseHandler : Failed with error. failure message!",
                fatalExcepCaptor.getValue().getMessage());

        ArgumentCaptor<HttpFailureException> resultFutureCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(resultFuture, times(1)).completeExceptionally(resultFutureCaptor.capture());
        assertEquals("PgResponseHandler : Failed with error. failure message!",
                resultFutureCaptor.getValue().getMessage());

        verify(meterStatsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldReportNonFatalExceptionAndCompleteWhenEventComesToFailureHandleAndFailOnErrorsIsFalse() {
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        when(event.succeeded()).thenReturn(false);
        when(event.cause()).thenReturn(new Exception("failure message!"));

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        ArgumentCaptor<HttpFailureException> nonFatalExcepCaptor = ArgumentCaptor.forClass(HttpFailureException.class);
        verify(errorReporter, times(1)).reportNonFatalException(nonFatalExcepCaptor.capture());
        assertEquals("PgResponseHandler : Failed with error. failure message!",
                nonFatalExcepCaptor.getValue().getMessage());
        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldPopulateResultAsObjectIfTypeIsNotPassedAndRetainResponseTypeIsTrue() {
        when(event.succeeded()).thenReturn(true);
        when(event.result()).thenReturn(resultRowSet);
        when(resultRowSet.iterator()).thenReturn(rowSetIterator);
        when(rowSetIterator.hasNext()).thenReturn(true).thenReturn(false);
        when(rowSetIterator.next()).thenReturn(row);
        when(row.getValue("surge_factor")).thenReturn("123");
        descriptor = TestBookingLogMessage.getDescriptor();
        inputColumnNames = new String[]{"s2_id"};
        outputColumnNames = new ArrayList<>();
        outputColumnNames.add("s2_id");
        outputColumnNames.add("surge_factor");
        outputMapping = new HashMap<>();
        outputMapping.put("surge_factor", "surge_factor");
        streamData = new Row(2);
        inputData = new Row(1);
        inputData.setField(0, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(outputColumnNames.size()));
        rowManager = new RowManager(streamData);
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        pgSourceConfig = getPgSourceConfigBuilder()
                .setType(null)
                .setQueryVariables("s2_id")
                .setQueryPattern("select surge_factor from public.surge where s2_id = '%s'")
                .setMetricId("")
                .setRetainResponseType(true)
                .createPgSourceConfig();
        Row outputStreamRow = new Row(2);
        outputStreamRow.setField(0, inputData);
        Row outputRow = new Row(2);
        outputRow.setField(1, "123");
        outputStreamRow.setField(1, outputRow);
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamRow));
        verify(meterStatsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));

    }

    @Test
    public void shouldNotPopulateResultAsObjectIfTypeIsNotPassedAndRetainResponseTypeIsFalse() {
        when(event.succeeded()).thenReturn(true);
        when(event.result()).thenReturn(resultRowSet);
        when(resultRowSet.iterator()).thenReturn(rowSetIterator);
        when(rowSetIterator.hasNext()).thenReturn(true).thenReturn(false);
        when(rowSetIterator.next()).thenReturn(row);
        when(row.getValue("surge_factor")).thenReturn("123");
        descriptor = TestSurgeFactorLogMessage.getDescriptor();
        inputColumnNames = new String[]{"s2_id"};
        outputColumnNames = new ArrayList<>();
        outputColumnNames.add("s2_id");
        outputColumnNames.add("surge_factor");
        outputMapping = new HashMap<>();
        outputMapping.put("surge_factor", "surge_factor");
        streamData = new Row(2);
        inputData = new Row(1);
        inputData.setField(0, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(outputColumnNames.size()));
        rowManager = new RowManager(streamData);
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        pgSourceConfig = getPgSourceConfigBuilder()
                .setType(null)
                .setQueryVariables("s2_id")
                .setQueryPattern("select surge_factor from public.surge where s2_id = '%s'")
                .setMetricId("")
                .setRetainResponseType(false)
                .createPgSourceConfig();
        Row outputStreamRow = new Row(2);
        outputStreamRow.setField(0, inputData);
        Row outputRow = new Row(2);
        outputRow.setField(1, 123.0f);
        outputStreamRow.setField(1, outputRow);
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter, new PostResponseTelemetry());
        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);
        verify(resultFuture, times(1)).complete(Collections.singleton(outputStreamRow));
        verify(meterStatsManager, times(1)).markEvent(SUCCESS_RESPONSE);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

}
