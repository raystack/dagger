package com.gojek.daggers.longbow;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongBowReaderTest {

    private HashMap<String, Integer> columnIndexMap = new HashMap<>();

    @Mock
    private Configuration configuration;

    @Mock
    private BigtableAsyncConnection bigtableAsyncConnection;

    @Mock
    private AsyncTable<AdvancedScanResultConsumer> asyncTable;

    @Mock
    private ResultFuture<Row> outputFuture;

    private LongBowSchema longBowSchema;
    private CompletableFuture<List<Result>> scanFuture;
    private Timestamp currentTimestamp;


    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString("LONGBOW_GCP_PROJECT_ID", "the-big-data-production-007")).thenReturn("test-project");
        when(configuration.getString("LONGBOW_GCP_INSTANCE_ID", "de-prod")).thenReturn("test-instance");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn("test-job");

        when(bigtableAsyncConnection.getTable(TableName.valueOf(Bytes.toBytes("test-job")))).thenReturn(asyncTable);
        scanFuture = new CompletableFuture<>();
        currentTimestamp = new Timestamp(System.currentTimeMillis());

        columnIndexMap.put("longbow_key", 0);
        columnIndexMap.put("longbow_data1", 1);
        columnIndexMap.put("rowtime", 2);
        columnIndexMap.put("longbow_duration", 3);

        longBowSchema = new LongBowSchema(columnIndexMap);
    }

    @Test
    public void shouldPopulateOutputWithAllTheInputFieldsWhenResultIsEmpty() throws Exception {
        scanFuture = CompletableFuture.supplyAsync(ArrayList::new);
        Row input = getRow("driver0", "order1", currentTimestamp, "24h");
        LongBowReader longBowReader = new LongBowReader(configuration, longBowSchema, bigtableAsyncConnection);
        when(asyncTable.scanAll(any(Scan.class))).thenReturn(scanFuture);

        longBowReader.open(configuration);
        longBowReader.asyncInvoke(input, outputFuture);

        verify(outputFuture).complete(argThat(rows -> verifyRow(rows, row -> {
            Assert.assertEquals(4, row.getArity());
            Assert.assertEquals("driver0", row.getField(0));
            Assert.assertEquals(getData(), row.getField(1));
            Assert.assertEquals(currentTimestamp, row.getField(2));
            Assert.assertEquals("24h", row.getField(3));
        })));
        Assert.assertTrue(scanFuture.isDone());
    }

    @Test
    public void shouldPopulateOutputWithResults() throws Exception {
        List<Result> results = getResults(getKeyValue("driver0", "longbow_data1", "order1"));
        scanFuture = CompletableFuture.supplyAsync(() -> results);
        LongBowReader longBowReader = new LongBowReader(configuration, longBowSchema, bigtableAsyncConnection);
        Row input = getRow("driver0", "order1", currentTimestamp, "24h");
        when(asyncTable.scanAll(any(Scan.class))).thenReturn(scanFuture);

        longBowReader.open(configuration);
        longBowReader.asyncInvoke(input, outputFuture);

        verify(outputFuture).complete(argThat(rows -> verifyRow(rows, row -> {
            Assert.assertEquals(4, row.getArity());
            Assert.assertEquals(getData("order1"), row.getField(1));
        })));
        Assert.assertTrue(scanFuture.isDone());
    }

    @Test
    public void shouldPopulateOutputWithMultipleResults() throws Exception {
        columnIndexMap.put("longbow_data2", 4);
        List<Result> results = getResults(getKeyValue("driver0", "longbow_data1", "order1"), getKeyValue("driver0", "longbow_data2", "order2"));
        scanFuture = CompletableFuture.supplyAsync(() -> results);
        LongBowReader longBowReader = new LongBowReader(configuration, longBowSchema, bigtableAsyncConnection);
        Row input = getRow("driver0", "order1", currentTimestamp, "24h", "order2");
        when(asyncTable.scanAll(any(Scan.class))).thenReturn(scanFuture);

        longBowReader.open(configuration);
        longBowReader.asyncInvoke(input, outputFuture);

        verify(outputFuture).complete(argThat(rows -> verifyRow(rows, row -> {
            Assert.assertEquals(5, row.getArity());
            Assert.assertEquals(getData("order1"), row.getField(1));
            Assert.assertEquals(getData("order2"), row.getField(4));
        })));
        Assert.assertTrue(scanFuture.isDone());
    }

    private ArrayList<Object> getData(String... orderDetails) {
        ArrayList<Object> data = new ArrayList<>();
        Collections.addAll(data, orderDetails);
        return data;
    }

    private Boolean verifyRow(Collection<Row> rows, Consumer<Row> assertResult) {
        Row row = rows.iterator().next();
        assertResult.accept(row);
        return true;
    }

    private Row getRow(Object... dataList) {
        Row input = new Row(dataList.length);
        for (int i = 0; i < dataList.length; i++) {
            input.setField(i, dataList[i]);
        }
        return input;
    }

    private List<Result> getResults(KeyValue... cells) {
        ArrayList<Result> results = new ArrayList<>();
        results.add(Result.create(cells));
        return results;
    }

    private KeyValue getKeyValue(String key, String columnName, String value) {
        return new KeyValue(Bytes.toBytes(key), Bytes.toBytes("ts"), Bytes.toBytes(columnName), Bytes.toBytes(value));
    }

}