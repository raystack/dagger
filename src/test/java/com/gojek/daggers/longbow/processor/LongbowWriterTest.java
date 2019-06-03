package com.gojek.daggers.longbow.processor;


import com.gojek.daggers.longbow.LongbowSchema;
import com.gojek.daggers.longbow.LongbowStore;
import com.gojek.daggers.longbow.metric.LongbowAspects;
import com.gojek.daggers.utils.stats.StatsManager;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.threeten.bp.Duration;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowWriterTest {

    @Mock
    private Configuration configuration;

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private LongbowStore longBowStore;

    @Mock
    private StatsManager statsManager;

    private String daggerID = "FR-DR-2116";
    private String longbowData1 = "RB-9876";
    private String longbowDuration = "1d";
    private String longbowKey = "rule123#driver444";
    private Timestamp longbowRowtime = new Timestamp(1558498933);

    private LongbowWriter defaultLongbowWriter;
    private LongbowSchema defaultLongbowSchema;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        MetricGroup metricGroup = mock(MetricGroup.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(any())).thenReturn(metricGroup);
        when(metricGroup.meter(any(), any())).thenReturn(mock(Meter.class));
        when(configuration.getString("LONGBOW_GCP_PROJECT_ID", "the-big-data-production-007")).thenReturn("test-project");
        when(configuration.getString("LONGBOW_GCP_INSTANCE_ID", "de-prod")).thenReturn("test-instance");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn(daggerID);
        when(configuration.getString("LONGBOW_DOCUMENT_DURATION", "90d")).thenReturn("90d");
        when(longBowStore.groupName()).thenReturn("test-group");
        when(longBowStore.tableName()).thenReturn(daggerID);

        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime"};
        defaultLongbowSchema = new LongbowSchema(columnNames);
        defaultLongbowWriter = new LongbowWriter(configuration, defaultLongbowSchema, statsManager, longBowStore);
        defaultLongbowWriter.setRuntimeContext(runtimeContext);
    }

    @Test
    public void shouldCreateTableWhenTableDoesNotExist() throws Exception {
        when(longBowStore.tableExists()).thenReturn(false);

        defaultLongbowWriter.open(configuration);

        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;
        verify(longBowStore, times(1)).tableExists();
        verify(longBowStore, times(1)).createTable(Duration.ofMillis(nintyDays), "ts");
        verify(statsManager, times(1)).markEvent(LongbowAspects.SUCCESS_ON_CREATE_BIGTABLE);
        verify(statsManager, times(1)).updateHistogram(eq(LongbowAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldNotCreateTableWhenTableExist() throws Exception {
        when(longBowStore.tableExists()).thenReturn(true);

        defaultLongbowWriter.open(configuration);

        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;
        verify(longBowStore, times(1)).tableExists();
        verify(longBowStore, times(0)).createTable(Duration.ofMillis(nintyDays), "ts");
        verify(statsManager, times(0)).markEvent(LongbowAspects.SUCCESS_ON_CREATE_BIGTABLE);
        verify(statsManager, times(0)).updateHistogram(eq(LongbowAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldInitializeLongBowStore() throws Exception {
        when(longBowStore.tableExists()).thenReturn(true);

        defaultLongbowWriter.open(configuration);

        verify(longBowStore, times(1)).initialize();
    }

    @Test
    public void shouldWriteToBigTableWithExpectedValue() throws Exception {
        Row input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);

        when(longBowStore.tableExists()).thenReturn(true);
        when(longBowStore.put(any(Put.class))).thenReturn(CompletableFuture.completedFuture(null));

        defaultLongbowWriter.open(configuration);
        defaultLongbowWriter.asyncInvoke(input, resultFuture);

        ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
        verify(longBowStore, times(1)).put(captor.capture());
        Put actualPut = captor.getValue();
        Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223372035296276874"))
                .addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1"), Bytes.toBytes(longbowData1));

        Assert.assertEquals(new String(expectedPut.getRow()), new String(actualPut.getRow()));
        Assert.assertEquals(expectedPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1")),
                actualPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1")));

        verify(resultFuture, times(1)).complete(Collections.singleton(input));
        verify(statsManager, times(1)).markEvent(LongbowAspects.SUCCESS_ON_WRITE_DOCUMENT);
        verify(statsManager, times(1)).updateHistogram(eq(LongbowAspects.SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldWriteToBigTableWithExpectedMultipleValues() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowWriter longBowWriter = new LongbowWriter(configuration, longBowSchema, statsManager, longBowStore);

        Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        String longbowData2 = "RB-4321";
        input.setField(4, longbowData2);

        when(longBowStore.tableExists()).thenReturn(true);
        when(longBowStore.put(any(Put.class))).thenReturn(CompletableFuture.completedFuture(null));

        longBowWriter.open(configuration);
        longBowWriter.asyncInvoke(input, resultFuture);

        ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
        verify(longBowStore, times(1)).put(captor.capture());
        Put actualPut = captor.getValue();
        Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223372035296276874"))
                .addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1"), Bytes.toBytes(longbowData1))
                .addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data2"), Bytes.toBytes(longbowData2));

        Assert.assertEquals(new String(expectedPut.getRow()), new String(actualPut.getRow()));
        Assert.assertEquals(expectedPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1")),
                actualPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1")));
        Assert.assertEquals(expectedPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data2")),
                actualPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data2")));

        verify(resultFuture, times(1)).complete(Collections.singleton(input));
        verify(statsManager, times(1)).markEvent(LongbowAspects.SUCCESS_ON_WRITE_DOCUMENT);
        verify(statsManager, times(1)).updateHistogram(eq(LongbowAspects.SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME), any(Long.class));
    }

    @Test(expected = RuntimeException.class)
    public void shouldCaptureExceptionWithStatsDManagerAndRethrowExceptionOnCreateTableFailure() throws Exception {
        long nintyDays = (long) 90 * 24 * 60 * 60 * 1000;

        when(longBowStore.tableExists()).thenReturn(false);
        doThrow(new RuntimeException()).when(longBowStore).createTable(Duration.ofMillis(nintyDays), "ts");

        defaultLongbowWriter.open(configuration);

        verify(statsManager, times(1)).markEvent(LongbowAspects.FAILURES_ON_CREATE_BIGTABLE);
        verify(statsManager, times(1)).updateHistogram(eq(LongbowAspects.FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME), any(Long.class));
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
        when(longBowStore.put(any(Put.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        defaultLongbowWriter.open(configuration);
        defaultLongbowWriter.asyncInvoke(input, resultFuture);

        verify(statsManager, times(1)).markEvent(LongbowAspects.FAILURES_ON_WRITE_DOCUMENT);
        verify(statsManager, times(1)).updateHistogram(eq(LongbowAspects.FAILURES_ON_WRITE_DOCUMENT_RESPONSE_TIME), any(Long.class));
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
        when(longBowStore.put(any(Put.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        defaultLongbowWriter.timeout(new Row(1), resultFuture);

        verify(statsManager, times(1)).markEvent(LongbowAspects.TIMEOUTS_ON_WRITER);
    }
}
