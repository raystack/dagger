package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.LongbowWriterAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.LongbowStore;
import com.gojek.daggers.postProcessors.longbow.exceptions.LongbowWriterException;
import com.gojek.daggers.postProcessors.longbow.storage.PutRequest;
import com.gojek.daggers.sink.ProtoSerializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.threeten.bp.Duration;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.utils.Constants.LONGBOW_VERSION_DEFAULT;
import static com.gojek.daggers.utils.Constants.LONGBOW_VERSION_KEY;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowWriterTest {

    @Mock
    private Configuration configuration;

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private LongbowStore longBowStore;

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private MeterStatsManager meterStatsManager;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private ProtoSerializer protoSerializer;

    @Mock
    private TelemetrySubscriber telemetrySubscriber;

    private String daggerID = "FR-DR-2116";
    private String longbowData1 = "RB-9876";
    private String longbowDuration = "1d";
    private String longbowKey = "rule123#driver444";
    private Timestamp longbowRowtime = new Timestamp(1558498933);

    private LongbowWriter defaultLongbowWriter;
    private LongbowSchema defaultLongbowSchema;
    private PutRequestFactory putRequestFactory;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        when(configuration.getString("LONGBOW_GCP_PROJECT_ID", "the-big-data-production-007"))
                .thenReturn("test-project");
        when(configuration.getString("LONGBOW_GCP_INSTANCE_ID", "de-prod")).thenReturn("test-instance");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn(daggerID);
        when(configuration.getString("LONGBOW_DOCUMENT_DURATION", "90d")).thenReturn("90d");
        when(configuration.getString(LONGBOW_VERSION_KEY, LONGBOW_VERSION_DEFAULT)).thenReturn("1");
        when(longBowStore.tableName()).thenReturn(daggerID);

        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime"};
        defaultLongbowSchema = new LongbowSchema(columnNames);
        putRequestFactory = new PutRequestFactory(defaultLongbowSchema, configuration, protoSerializer);
        defaultLongbowWriter = new LongbowWriter(configuration, defaultLongbowSchema, meterStatsManager, errorReporter,
                longBowStore, putRequestFactory);
        defaultLongbowWriter.setRuntimeContext(runtimeContext);
    }

    @Test
    public void shouldCreateTableWhenTableDoesNotExist() throws Exception {
        when(longBowStore.tableExists()).thenReturn(false);

        defaultLongbowWriter.open(configuration);

        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;
        verify(longBowStore, times(1)).tableExists();
        verify(longBowStore, times(1)).createTable(Duration.ofMillis(nintyDays), "ts");
        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE);
        verify(meterStatsManager, times(1))
                .updateHistogram(eq(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldNotCreateTableWhenTableExist() throws Exception {
        when(longBowStore.tableExists()).thenReturn(true);

        defaultLongbowWriter.open(configuration);

        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;
        verify(longBowStore, times(1)).tableExists();
        verify(longBowStore, times(0)).createTable(Duration.ofMillis(nintyDays), "ts");
        verify(meterStatsManager, times(0)).markEvent(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE);
        verify(meterStatsManager, times(0))
                .updateHistogram(eq(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldInitializeLongBowStore() throws Exception {
        when(longBowStore.tableExists()).thenReturn(true);

        defaultLongbowWriter.open(configuration);

        verify(longBowStore, times(1)).initialize();
    }

    @Test
    public void shouldWriteToBigTable() throws Exception {
        Row input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);

        when(longBowStore.tableExists()).thenReturn(true);
        when(longBowStore.put(any(PutRequest.class))).thenReturn(CompletableFuture.completedFuture(null));

        defaultLongbowWriter.open(configuration);
        defaultLongbowWriter.asyncInvoke(input, resultFuture);

        verify(resultFuture, times(1)).complete(Collections.singleton(input));
        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.SUCCESS_ON_WRITE_DOCUMENT);
        verify(meterStatsManager, times(1))
                .updateHistogram(eq(LongbowWriterAspects.SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME), any(Long.class));
    }

    @Test(expected = RuntimeException.class)
    public void shouldCaptureExceptionWithStatsDManagerAndRethrowExceptionOnCreateTableFailure() throws Exception {
        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;

        when(longBowStore.tableExists()).thenReturn(false);
        doThrow(new RuntimeException()).when(longBowStore).createTable(Duration.ofMillis(nintyDays), "ts");

        defaultLongbowWriter.open(configuration);

        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.FAILURES_ON_CREATE_BIGTABLE);
        verify(meterStatsManager, times(1))
                .updateHistogram(eq(LongbowWriterAspects.FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldCaptureExceptionWithStatsDManagerOnWriteDocumentFailure() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        defaultLongbowSchema = new LongbowSchema(columnNames);

        Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        String longbowData2 = "RB-4321";
        input.setField(4, longbowData2);

        when(longBowStore.tableExists()).thenReturn(true);
        when(longBowStore.put(any(PutRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        defaultLongbowWriter.open(configuration);
        defaultLongbowWriter.asyncInvoke(input, resultFuture);

        verify(errorReporter, times(1)).reportNonFatalException(any(LongbowWriterException.class));
        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.FAILED_ON_WRITE_DOCUMENT);
        verify(meterStatsManager, times(1))
                .updateHistogram(eq(LongbowWriterAspects.FAILED_ON_WRITE_DOCUMENT_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldCaptureTimeoutWithStatsDManager() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        defaultLongbowSchema = new LongbowSchema(columnNames);

        Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        String longbowData2 = "RB-4321";
        input.setField(4, longbowData2);

        when(longBowStore.tableExists()).thenReturn(true);
        when(longBowStore.put(any(PutRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        defaultLongbowWriter.timeout(new Row(1), resultFuture);

        verify(meterStatsManager, times(1)).markEvent(LongbowWriterAspects.TIMEOUTS_ON_WRITER);
        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("longbow_writer_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        putRequestFactory = new PutRequestFactory(longBowSchema, configuration, protoSerializer);
        LongbowWriter longBowWriter = new LongbowWriter(configuration, longBowSchema, meterStatsManager, errorReporter,
                longBowStore, putRequestFactory);

        longBowWriter.preProcessBeforeNotifyingSubscriber();
        Assert.assertEquals(metrics, longBowWriter.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        putRequestFactory = new PutRequestFactory(longBowSchema, configuration, protoSerializer);
        LongbowWriter longBowWriter = new LongbowWriter(configuration, longBowSchema, meterStatsManager, errorReporter,
                longBowStore, putRequestFactory);
        longBowWriter.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(longBowWriter);
    }

}
