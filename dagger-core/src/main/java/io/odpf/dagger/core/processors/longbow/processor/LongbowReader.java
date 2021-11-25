package io.odpf.dagger.core.processors.longbow.processor;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.LongbowReaderAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.data.LongbowData;
import io.odpf.dagger.core.processors.longbow.exceptions.LongbowReaderException;
import io.odpf.dagger.core.processors.longbow.outputRow.ReaderOutputRow;
import io.odpf.dagger.core.processors.longbow.range.LongbowRange;
import io.odpf.dagger.core.processors.longbow.request.ScanRequestFactory;
import io.odpf.dagger.core.processors.longbow.storage.LongbowStore;
import io.odpf.dagger.core.processors.longbow.storage.ScanRequest;
import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.time.Duration.between;

/**
 * The Longbow reader.
 */
public class LongbowReader extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(LongbowReader.class.getName());
    private Configuration configuration;
    private LongbowSchema longBowSchema;
    private LongbowRange longbowRange;
    private LongbowStore longBowStore;
    private MeterStatsManager meterStatsManager;
    private Map<String, List<String>> metrics = new HashMap<>();
    private ErrorReporter errorReporter;
    private LongbowData longbowData;
    private ScanRequestFactory scanRequestFactory;
    private ReaderOutputRow readerOutputRow;

    /**
     * Instantiates a new Longbow reader with specified longbow store.
     *
     * @param configuration      the configuration
     * @param longBowSchema      the longbow schema
     * @param longbowRange       the longbow range
     * @param longBowStore       the longbow store
     * @param meterStatsManager  the meter stats manager
     * @param errorReporter      the error reporter
     * @param longbowData        the longbow data
     * @param scanRequestFactory the scan request factory
     * @param readerOutputRow    the reader output row
     */
    LongbowReader(Configuration configuration, LongbowSchema longBowSchema, LongbowRange longbowRange, LongbowStore longBowStore, MeterStatsManager meterStatsManager, ErrorReporter errorReporter, LongbowData longbowData, ScanRequestFactory scanRequestFactory, ReaderOutputRow readerOutputRow) {
        this(configuration, longBowSchema, longbowRange, longbowData, scanRequestFactory, readerOutputRow);
        this.longBowStore = longBowStore;
        this.meterStatsManager = meterStatsManager;
        this.errorReporter = errorReporter;
    }

    /**
     * Instantiates a new Longbow reader.
     *
     * @param configuration      the configuration
     * @param longBowSchema      the longbow schema
     * @param longbowRange       the longbow range
     * @param longbowData        the longbow data
     * @param scanRequestFactory the scan request factory
     * @param readerOutputRow    the reader output row
     */
    public LongbowReader(Configuration configuration, LongbowSchema longBowSchema, LongbowRange longbowRange, LongbowData longbowData, ScanRequestFactory scanRequestFactory, ReaderOutputRow readerOutputRow) {
        this.configuration = configuration;
        this.longBowSchema = longBowSchema;
        this.longbowRange = longbowRange;
        this.longbowData = longbowData;
        this.scanRequestFactory = scanRequestFactory;
        this.readerOutputRow = readerOutputRow;
    }


    @Override
    public void open(org.apache.flink.configuration.Configuration internalFlinkConfig) throws Exception {
        super.open(internalFlinkConfig);
        if (longBowStore == null) {
            longBowStore = LongbowStore.create(configuration);
        }
        if (meterStatsManager == null) {
            meterStatsManager = new MeterStatsManager(getRuntimeContext().getMetricGroup(), true);
        }
        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext().getMetricGroup(), configuration);
        }
        meterStatsManager.register("longbow.reader", LongbowReaderAspects.values());
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(TelemetryTypes.POST_PROCESSOR_TYPE.getValue(), Constants.LONGBOW_READER_PROCESSOR_KEY);
    }

    @Override
    public void close() throws Exception {
        super.close();
        meterStatsManager.markEvent(LongbowReaderAspects.CLOSE_CONNECTION_ON_READER);
        LOGGER.error("LongbowReader : Connection closed");
        if (longBowStore != null) {
            longBowStore.close();
        }
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        ScanRequest scanRequest = scanRequestFactory.create(input, longbowRange);
        Instant startTime = Instant.now();
        longBowStore.scanAll(scanRequest)
                .exceptionally(throwable -> logException(throwable, startTime))
                .thenAccept(scanResult -> {
                    instrumentation(scanResult, startTime, input);
                    Row row = readerOutputRow.get(longbowData.parse(scanResult), input);
                    resultFuture.complete(Collections.singletonList(row));
                });
    }

    /**
     * Gets longbow range.
     *
     * @return the longbow range
     */
    public LongbowRange getLongbowRange() {
        return longbowRange;
    }

    private void instrumentation(List<Result> scanResult, Instant startTime, Row input) {
        meterStatsManager.markEvent(LongbowReaderAspects.SUCCESS_ON_READ_DOCUMENT);
        meterStatsManager.updateHistogram(LongbowReaderAspects.SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        meterStatsManager.updateHistogram(LongbowReaderAspects.DOCUMENTS_READ_PER_SCAN, scanResult.size());
        if (scanResult.isEmpty() || !Arrays.equals(scanResult.get(0).getRow(), longBowSchema.getKey(input, 0))) {
            meterStatsManager.markEvent(LongbowReaderAspects.FAILED_TO_READ_LAST_RECORD);
        }
    }

    private List<Result> logException(Throwable ex, Instant startTime) {
        LOGGER.error("LongbowReader : failed to scan document from BigTable: {}", ex.getMessage());
        ex.printStackTrace();
        meterStatsManager.markEvent(LongbowReaderAspects.FAILED_ON_READ_DOCUMENT);
        errorReporter.reportNonFatalException(new LongbowReaderException(ex));
        meterStatsManager.updateHistogram(LongbowReaderAspects.FAILED_ON_READ_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        return Collections.emptyList();
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) {
        LOGGER.error("LongbowReader : timeout when reading document");
        meterStatsManager.markEvent(LongbowReaderAspects.TIMEOUTS_ON_READER);
        Exception timeoutException = new TimeoutException("Async function call has timed out.");
        errorReporter.reportFatalException(timeoutException);
        resultFuture.completeExceptionally(timeoutException);
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
