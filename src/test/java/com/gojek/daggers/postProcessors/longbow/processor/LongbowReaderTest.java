package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.data.LongbowData;
import com.gojek.daggers.postProcessors.longbow.exceptions.LongbowReaderException;
import com.gojek.daggers.postProcessors.longbow.outputRow.OutputRow;
import com.gojek.daggers.postProcessors.longbow.request.ScanRequestFactory;
import com.gojek.daggers.postProcessors.longbow.row.LongbowAbsoluteRange;
import com.gojek.daggers.postProcessors.longbow.storage.LongbowStore;
import com.gojek.daggers.postProcessors.longbow.storage.ScanRequest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
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
import static com.gojek.daggers.utils.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowReaderTest {

    @Mock
    ResultFuture<Row> resultFuture;
    @Mock
    LongbowData longbowData;
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
    private LongbowAbsoluteRange longbowAbsoluteRow;
    @Mock
    private TelemetrySubscriber telemetrySubscriber;
    @Mock
    private OutputRow outputRow;
    private LongbowSchema longBowSchema;
    private Timestamp currentTimestamp;
    private ScanRequestFactory scanRequestFactory;
    private String tableId = "tableId";


    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString(LONGBOW_GCP_PROJECT_ID_KEY, LONGBOW_GCP_PROJECT_ID_DEFAULT)).thenReturn("test-project");
        when(configuration.getString(LONGBOW_GCP_INSTANCE_ID_KEY, LONGBOW_GCP_INSTANCE_ID_DEFAULT)).thenReturn("test-instance");
        when(configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT)).thenReturn("test-job");
        currentTimestamp = new Timestamp(System.currentTimeMillis());
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration"};
        longBowSchema = new LongbowSchema(columnNames);
        scanRequestFactory = new ScanRequestFactory(longBowSchema, tableId);
    }

    @Test
    public void shouldCaptureExceptionWithStatsDManagerOnReadDocumentFailure() throws Exception {
        Row input = getRow("driver0", "order1", currentTimestamp, "24h");
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, outputRow);
        when(longBowStore.scanAll(any(ScanRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        longBowReader.open(configuration);
        longBowReader.asyncInvoke(input, outputFuture);

        verify(meterStatsManager, times(1)).markEvent(FAILED_ON_READ_DOCUMENT);
        verify(errorReporter, times(1)).reportNonFatalException(any(LongbowReaderException.class));
    }

    @Test
    public void shouldHandleClose() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, outputRow);

        longBowReader.close();

        verify(longBowStore, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_READER);
    }

    @Test
    public void shouldReturnLongbowRow() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, outputRow);
        Assert.assertEquals(longBowReader.getLongbowRange(), longbowAbsoluteRow);
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("longbow_reader_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, outputRow);

        longBowReader.preProcessBeforeNotifyingSubscriber();
        Assert.assertEquals(metrics, longBowReader.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, outputRow);
        longBowReader.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(longBowReader);
    }

    @Test
    public void shouldFailOnTimeout() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, outputRow);
        Row input = getRow("driver0", "order1", currentTimestamp, "24h");

        longBowReader.timeout(input, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(TIMEOUTS_ON_READER);
        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }

    private Row getRow(Object... dataList) {
        Row input = new Row(dataList.length);
        for (int i = 0; i < dataList.length; i++) {
            input.setField(i, dataList[i]);
        }
        return input;
    }
}