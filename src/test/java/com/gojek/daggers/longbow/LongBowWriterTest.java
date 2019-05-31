package com.gojek.daggers.longbow;

import com.gojek.daggers.async.metric.StatsManager;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class LongBowWriterTest {

    @Mock
    private AsyncTable<AdvancedScanResultConsumer> asyncTable;

    @Mock
    private BigtableAsyncConnection bigtableClient;

    @Mock
    private BigtableTableAdminClient bigtableTableAdminClient;

    @Mock
    private Configuration configuration;

    @Mock
    private ResultFuture<Row> resultFuture;

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private Table syncTable;

    private HashMap<String, Integer> columnIndexMap = new HashMap<>();
    private String groupName = "bigtable.test";
    private String daggerID = "FR-DR-2116";
    private String longbowData1 = "RB-9876";
    private String longbowData2 = "RB-4321";
    private String longbowDuration = "1d";
    private String longbowKey = "rule123#driver444";
    private Timestamp longbowRowtime = new Timestamp(1558498933);
    private TableName tableName = TableName.valueOf(daggerID);

    private LongBowWriter longbowWriter;
    private LongBowSchema longbowSchema;
    private StatsManager statsManager;

    @Before
    public void setUp() throws Exception {
        MetricGroup metricGroup = mock(MetricGroup.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(any())).thenReturn(metricGroup);
        when(metricGroup.meter(any(), any())).thenReturn(mock(Meter.class));
        when(configuration.getString("LONGBOW_GCP_PROJECT_ID", "the-big-data-production-007")).thenReturn("test-project");
        when(configuration.getString("LONGBOW_GCP_INSTANCE_ID", "de-prod")).thenReturn("test-instance");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn(daggerID);
        when(configuration.getString("LONGBOW_DOCUMENT_DURATION", "90d")).thenReturn("90d");

        columnIndexMap.put("longbow_key", 0);
        columnIndexMap.put("longbow_data1", 1);
        columnIndexMap.put("longbow_duration", 2);
        columnIndexMap.put("rowtime", 3);

        longbowSchema = new LongBowSchema(columnIndexMap);
        longbowWriter = new LongBowWriter(configuration, longbowSchema, bigtableClient, bigtableTableAdminClient);
        statsManager = new StatsManager(runtimeContext, groupName, true);

        longbowWriter.setRuntimeContext(runtimeContext);
        longbowWriter.setStatsManager(statsManager);
    }

    @Test
    public void shouldCallCreateTableWhenTableDoesNotExist() throws Exception {
        Row input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);

        when(bigtableTableAdminClient.exists(daggerID)).thenReturn(false);
        when(bigtableTableAdminClient.createTable(any(CreateTableRequest.class))).thenReturn(syncTable);
        when(bigtableClient.getTable(tableName)).thenReturn(asyncTable);

        longbowWriter.open(configuration);

        verify(bigtableTableAdminClient, times(1)).exists(daggerID);
        verify(bigtableTableAdminClient, times(1)).createTable(any(CreateTableRequest.class));
    }

    @Test
    public void shouldNotCreateBigtableOnTimeout() throws Exception {
        Row input = new Row(1);
        Row value = mock(Row.class);
        input.setField(0, value);

        when(bigtableTableAdminClient.exists(daggerID)).thenReturn(true);

        longbowWriter.open(configuration);
        longbowWriter.timeout(input, resultFuture);

        verify(bigtableTableAdminClient, times(1)).exists(daggerID);
        verify(bigtableTableAdminClient, times(0)).createTable(any(CreateTableRequest.class));
        verify(resultFuture, times(1)).complete(Collections.singleton(input));
    }

    @Test
    public void shouldWriteToBigtableWithExpectedValue() throws Exception {
        Row input = new Row(4);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);

        when(bigtableTableAdminClient.exists(daggerID)).thenReturn(true);
        when(bigtableClient.getTable(tableName)).thenReturn(asyncTable);
        when(asyncTable.put(any(Put.class))).thenReturn(CompletableFuture.completedFuture(null));

        longbowWriter.open(configuration);
        longbowWriter.asyncInvoke(input, resultFuture);

        ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
        verify(asyncTable, times(1)).put(captor.capture());
        Put actualPut = captor.getValue();
        Put expectedPut = new Put(Bytes.toBytes(longbowKey + "#9223372035296276874"))
                .addColumn(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1"), Bytes.toBytes(longbowData1));

        Assert.assertEquals(new String(expectedPut.getRow()), new String(actualPut.getRow()));
        Assert.assertEquals(expectedPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1")),
                actualPut.get(Bytes.toBytes("ts"), Bytes.toBytes("longbow_data1")));

        verify(resultFuture, times(1)).complete(Collections.singleton(input));
    }

    @Test
    public void shouldWriteToBigtableWithExpectedMultipleValues() throws Exception {
        columnIndexMap.put("longbow_data2", 4);

        Row input = new Row(5);
        input.setField(0, longbowKey);
        input.setField(1, longbowData1);
        input.setField(2, longbowDuration);
        input.setField(3, longbowRowtime);
        input.setField(4, longbowData2);

        when(bigtableTableAdminClient.exists(daggerID)).thenReturn(true);
        when(bigtableClient.getTable(tableName)).thenReturn(asyncTable);
        when(asyncTable.put(any(Put.class))).thenReturn(CompletableFuture.completedFuture(null));

        longbowWriter.open(configuration);
        longbowWriter.asyncInvoke(input, resultFuture);

        ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
        verify(asyncTable, times(1)).put(captor.capture());
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
    }
}
