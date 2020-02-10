package com.gojek.daggers.postProcessors.longbow.processor;


import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.LongbowStore;
import com.gojek.daggers.postProcessors.longbow.exceptions.LongbowReaderException;
import com.gojek.daggers.postProcessors.longbow.row.LongbowAbsoluteRow;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.gojek.daggers.metrics.aspects.LongbowReaderAspects.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowReaderTest {

    @Mock
    private Configuration configuration;

    @Mock
    private LongbowStore longBowStore;

    @Mock
    private ResultFuture<Row> outputFuture;

    @Mock
    private MeterStatsManager meterStatsManager;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private LongbowAbsoluteRow longbowAbsoluteRow;

    @Mock
    private TelemetrySubscriber telemetrySubscriber;

    @Mock
    ResultFuture<Row> resultFuture;

    @Mock
    LongbowData longbowData;

    private LongbowSchema longBowSchema;
    private CompletableFuture<List<Result>> scanFuture;
    private Timestamp currentTimestamp;


    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString("LONGBOW_GCP_PROJECT_ID", "the-big-data-production-007")).thenReturn("test-project");
        when(configuration.getString("LONGBOW_GCP_INSTANCE_ID", "de-prod")).thenReturn("test-instance");
        when(configuration.getString("FLINK_JOB_ID", "SQL Flink Job")).thenReturn("test-job");
        scanFuture = new CompletableFuture<>();
        currentTimestamp = new Timestamp(System.currentTimeMillis());
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration"};
        longBowSchema = new LongbowSchema(columnNames);
    }

    @Test
    public void shouldPopulateOutputWithAllTheInputFieldsWhenResultIsEmpty() throws Exception {
        scanFuture = CompletableFuture.supplyAsync(ArrayList::new);
        Row input = getRow("driver0", "order1", currentTimestamp, "24h");
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);
        when(longBowStore.scanAll(any(Scan.class))).thenReturn(scanFuture);
        Map data = new HashMap();
        data.put("longbow_data1", new ArrayList<>());
        when(longbowData.parse(any())).thenReturn(data);

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
        verify(meterStatsManager, times(1)).markEvent(SUCCESS_ON_READ_DOCUMENT);
        verify(meterStatsManager, times(1)).updateHistogram(eq(SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldCaptureExceptionWithStatsDManagerOnReadDocumentFailure() throws Exception {
        Row input = getRow("driver0", "order1", currentTimestamp, "24h");
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);
        when(longBowStore.scanAll(any(Scan.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        longBowReader.open(configuration);
        longBowReader.asyncInvoke(input, outputFuture);

        verify(meterStatsManager, times(1)).markEvent(FAILED_ON_READ_DOCUMENT);
        verify(errorReporter, times(1)).reportNonFatalException(any(LongbowReaderException.class));
    }


    @Test
    public void shouldPopulateOutputWithResults() throws Exception {
        List<Result> results = getResults(getKeyValue("driver0", "longbow_data1", "order1"));
        scanFuture = CompletableFuture.supplyAsync(() -> results);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);
        Row input = getRow("driver0", "order1", currentTimestamp, "24h");
        when(longBowStore.scanAll(any(Scan.class))).thenReturn(scanFuture);
        Map data = new HashMap();
        data.put("longbow_data1", Collections.singletonList("order1"));
        when(longbowData.parse(any())).thenReturn(data);

        longBowReader.open(configuration);
        longBowReader.asyncInvoke(input, outputFuture);

        verify(outputFuture).complete(argThat(rows -> verifyRow(rows, row -> {
            Assert.assertEquals(4, row.getArity());
            Assert.assertEquals(getData("order1"), row.getField(1));
        })));
        Assert.assertTrue(scanFuture.isDone());
        verify(meterStatsManager, times(1)).markEvent(SUCCESS_ON_READ_DOCUMENT);
        verify(meterStatsManager, times(1)).updateHistogram(eq(SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME), any(Long.class));
    }

    @Test
    public void shouldPopulateOutputWithMultipleResults() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);
        List<Result> results = getResults(getKeyValue("driver0", "longbow_data1", "order1"), getKeyValue("driver0", "longbow_data2", "order2"));
        scanFuture = CompletableFuture.supplyAsync(() -> results);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);
        Row input = getRow("driver0", "order1", currentTimestamp, "24h", "order2");
        when(longBowStore.scanAll(any(Scan.class))).thenReturn(scanFuture);

        Map data = new HashMap();
        data.put("longbow_data1", Collections.singletonList("order1"));
        data.put("longbow_data2", Collections.singletonList("order2"));
        when(longbowData.parse(any())).thenReturn(data);

        longBowReader.open(configuration);
        longBowReader.asyncInvoke(input, outputFuture);

        verify(outputFuture).complete(argThat(rows -> verifyRow(rows, row -> {
            Assert.assertEquals(5, row.getArity());
            Assert.assertEquals(getData("order1"), row.getField(1));
            Assert.assertEquals(getData("order2"), row.getField(4));
        })));
        Assert.assertTrue(scanFuture.isDone());
    }

    @Test
    public void shouldHandleClose() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);

        longBowReader.close();

        verify(longBowStore, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_READER);
    }

    @Test
    public void shouldReturnLongbowRow() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);
        Assert.assertEquals(longBowReader.getLongbowRow(), longbowAbsoluteRow);
    }


    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("longbow_reader_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);

        longBowReader.preProcessBeforeNotifyingSubscriber();
        Assert.assertEquals(metrics, longBowReader.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);
        longBowReader.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(longBowReader);
    }

    @Test
    public void shouldFailOnTimeout() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData);
        Row input = getRow("driver0", "order1", currentTimestamp, "24h");

        longBowReader.timeout(input, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(TIMEOUTS_ON_READER);
        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
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