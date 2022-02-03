package io.odpf.dagger.core.processors.longbow.processor;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.data.LongbowData;
import io.odpf.dagger.core.processors.longbow.exceptions.LongbowReaderException;
import io.odpf.dagger.core.processors.longbow.outputRow.ReaderOutputRow;
import io.odpf.dagger.core.processors.longbow.range.LongbowAbsoluteRange;
import io.odpf.dagger.core.processors.longbow.request.ScanRequestFactory;
import io.odpf.dagger.core.processors.longbow.storage.LongbowStore;
import io.odpf.dagger.core.processors.longbow.storage.ScanRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static io.odpf.dagger.core.metrics.aspects.LongbowReaderAspects.CLOSE_CONNECTION_ON_READER;
import static io.odpf.dagger.core.metrics.aspects.LongbowReaderAspects.FAILED_ON_READ_DOCUMENT;
import static io.odpf.dagger.core.metrics.aspects.LongbowReaderAspects.TIMEOUTS_ON_READER;
import static io.odpf.dagger.core.utils.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowReaderTest {

    @Mock
    private ResultFuture<Row> resultFuture;
    @Mock
    private LongbowData longbowData;
    @Mock
    private Configuration configuration;
    @Mock
    private org.apache.flink.configuration.Configuration flinkInternalConfig;
    @Mock
    private LongbowStore longBowStore;
    @Mock
    private MeterStatsManager meterStatsManager;
    @Mock
    private ErrorReporter errorReporter;
    @Mock
    private LongbowAbsoluteRange longbowAbsoluteRow;
    @Mock
    private TelemetrySubscriber telemetrySubscriber;
    @Mock
    private ReaderOutputRow readerOutputRow;

    private LongbowSchema defaultLongBowSchema;
    private Timestamp currentTimestamp;
    private ScanRequestFactory scanRequestFactory;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString(PROCESSOR_LONGBOW_GCP_PROJECT_ID_KEY, PROCESSOR_LONGBOW_GCP_PROJECT_ID_DEFAULT)).thenReturn("test-project");
        when(configuration.getString(PROCESSOR_LONGBOW_GCP_INSTANCE_ID_KEY, PROCESSOR_LONGBOW_GCP_INSTANCE_ID_DEFAULT)).thenReturn("test-instance");
        when(configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT)).thenReturn("test-job");
        currentTimestamp = new Timestamp(System.currentTimeMillis());
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration"};
        defaultLongBowSchema = new LongbowSchema(columnNames);
        String tableId = "tableId";
        scanRequestFactory = new ScanRequestFactory(defaultLongBowSchema, tableId);
    }

    @Test
    public void shouldCaptureExceptionWithStatsDManagerOnReadDocumentFailure() throws Exception {
        Row input = Row.of("driver0", "order1", currentTimestamp, "24h");
        LongbowReader longBowReader = new LongbowReader(configuration, defaultLongBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, readerOutputRow);
        when(longBowStore.scanAll(any(ScanRequest.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ResultFuture<Row> callback = new ResultFuture<Row>() {
            @Override
            public void complete(Collection<Row> result) {
                countDownLatch.countDown();
            }

            @Override
            public void completeExceptionally(Throwable error) {
            }
        };
        longBowReader.open(flinkInternalConfig);
        longBowReader.asyncInvoke(input, callback);
        countDownLatch.await();
        verify(meterStatsManager, times(1)).markEvent(FAILED_ON_READ_DOCUMENT);
        verify(errorReporter, times(1)).reportNonFatalException(any(LongbowReaderException.class));
    }

    @Test
    public void shouldHandleClose() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        defaultLongBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, defaultLongBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, readerOutputRow);

        longBowReader.close();

        verify(longBowStore, times(1)).close();
        verify(meterStatsManager, times(1)).markEvent(CLOSE_CONNECTION_ON_READER);
    }

    @Test
    public void shouldReturnLongbowRow() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        defaultLongBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, defaultLongBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, readerOutputRow);
        assertEquals(longbowAbsoluteRow, longBowReader.getLongbowRange());
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("longbow_reader_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);

        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, readerOutputRow);

        longBowReader.preProcessBeforeNotifyingSubscriber();
        assertEquals(metrics, longBowReader.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, readerOutputRow);
        longBowReader.notifySubscriber(telemetrySubscriber);

        verify(telemetrySubscriber, times(1)).updated(longBowReader);
    }

    @Test
    public void shouldFailOnTimeout() throws Exception {
        String[] columnNames = {"longbow_key", "longbow_data1", "longbow_duration", "rowtime", "longbow_data2"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        LongbowReader longBowReader = new LongbowReader(configuration, longBowSchema, longbowAbsoluteRow, longBowStore, meterStatsManager, errorReporter, longbowData, scanRequestFactory, readerOutputRow);
        Row input = Row.of("driver0", "order1", currentTimestamp, "24h");

        longBowReader.timeout(input, resultFuture);

        verify(meterStatsManager, times(1)).markEvent(TIMEOUTS_ON_READER);
        verify(errorReporter, times(1)).reportFatalException(any(TimeoutException.class));
        verify(resultFuture, times(1)).completeExceptionally(any(TimeoutException.class));
    }
}
