package com.gojek.daggers.postProcessors.external.pg;

import com.gojek.daggers.exception.HttpFailureException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.Aspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.esb.aggregate.surge.SurgeFactorLogMessage;
import com.google.protobuf.Descriptors;
import io.vertx.core.AsyncResult;
import io.vertx.sqlclient.RowSet;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class PgResponseHandlerTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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

    private Descriptors.Descriptor descriptor;
    private List<String> outputColumnNames;
    private String[] inputColumnNames;
    private HashMap<String, String> outputMapping;
    private Row streamData;
    private RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private Row inputData;
    private String metricId;

    @Before
    public void setup() {
        initMocks(this);
        descriptor = SurgeFactorLogMessage.getDescriptor();
        inputColumnNames = new String[0];
        outputColumnNames = new ArrayList<>();
        outputColumnNames.add("customer_url");
        outputColumnNames.add("activity_source");
        outputMapping = new HashMap<>();
        streamData = new Row(2);
        inputData = new Row(3);
        inputData.setField(1, "123456");
        streamData.setField(0, inputData);
        streamData.setField(1, new Row(2));
        rowManager = new RowManager(streamData);
        columnNameManager = new ColumnNameManager(inputColumnNames, outputColumnNames);
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "5432", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "customer_id", "select * from public.customers where customer_id = '%s'", false, metricId);
    }

    @Test
    public void shouldGoToSuccessHandlerAndMarkSuccessResponseIfEventSucceedsAndResultSetHasOnlyOneRow() {
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter);
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
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter);
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
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter);
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
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "5432", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "customer_id", "select * from public.customers where customer_id = '%s'", true, metricId);
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter);
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
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter);
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
        pgSourceConfig = new PgSourceConfig("10.0.60.227,10.0.60.229,10.0.60.228", "5432", "user", "password", "db", "com.gojek.esb.fraud.DriverProfileFlattenLogMessage", "30",
                "5000", outputMapping, "5000", "5000", "customer_id", "select * from public.customers where customer_id = '%s'", true, metricId);
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter);
        when(event.succeeded()).thenReturn(false);
        when(event.cause()).thenReturn(new Exception("failure message!"));

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        verify(errorReporter, times(1)).reportFatalException(any(HttpFailureException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(HttpFailureException.class));
        verify(meterStatsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

    @Test
    public void shouldReportNonFatalExceptionAndCompleteWhenEventComesToFailureHandleAndFailOnErrorsIsFalse() {
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager, columnNameManager, descriptor, resultFuture, errorReporter);
        when(event.succeeded()).thenReturn(false);
        when(event.cause()).thenReturn(new Exception("failure message!"));

        pgResponseHandler.startTimer();
        pgResponseHandler.handle(event);

        verify(errorReporter, times(1)).reportNonFatalException(any(HttpFailureException.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(streamData));
        verify(meterStatsManager, times(1)).markEvent(TOTAL_FAILED_REQUESTS);
        verify(meterStatsManager, times(1)).updateHistogram(any(Aspects.class), any(Long.class));
    }

}