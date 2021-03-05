package com.gojek.daggers.postprocessors.longbow.processor;

import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.LongbowReaderAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.postprocessors.longbow.LongbowSchema;
import com.gojek.daggers.postprocessors.longbow.data.LongbowData;
import com.gojek.daggers.postprocessors.longbow.exceptions.LongbowReaderException;
import com.gojek.daggers.postprocessors.longbow.outputRow.ReaderOutputRow;
import com.gojek.daggers.postprocessors.longbow.request.ScanRequestFactory;
import com.gojek.daggers.postprocessors.longbow.range.LongbowRange;
import com.gojek.daggers.postprocessors.longbow.storage.LongbowStore;
import com.gojek.daggers.postprocessors.longbow.storage.ScanRequest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.LongbowReaderAspects.*;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.utils.Constants.LONGBOW_READER_PROCESSOR;
import static java.time.Duration.between;

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

    LongbowReader(Configuration configuration, LongbowSchema longBowSchema, LongbowRange longbowRange, LongbowStore longBowStore, MeterStatsManager meterStatsManager, ErrorReporter errorReporter, LongbowData longbowData, ScanRequestFactory scanRequestFactory, ReaderOutputRow readerOutputRow) {
        this(configuration, longBowSchema, longbowRange, longbowData, scanRequestFactory, readerOutputRow);
        this.longBowStore = longBowStore;
        this.meterStatsManager = meterStatsManager;
        this.errorReporter = errorReporter;
    }

    public LongbowReader(Configuration configuration, LongbowSchema longBowSchema, LongbowRange longbowRange, LongbowData longbowData, ScanRequestFactory scanRequestFactory, ReaderOutputRow readerOutputRow) {
        this.configuration = configuration;
        this.longBowSchema = longBowSchema;
        this.longbowRange = longbowRange;
        this.longbowData = longbowData;
        this.scanRequestFactory = scanRequestFactory;
        this.readerOutputRow = readerOutputRow;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (longBowStore == null) {
            longBowStore = LongbowStore.create(configuration);
        }
        if (meterStatsManager == null) {
            meterStatsManager = new MeterStatsManager(getRuntimeContext(), true);
        }
        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext(), configuration);
        }
        meterStatsManager.register("longbow.reader", LongbowReaderAspects.values());
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(POST_PROCESSOR_TYPE.getValue(), LONGBOW_READER_PROCESSOR);
    }

    @Override
    public void close() throws Exception {
        super.close();
        meterStatsManager.markEvent(CLOSE_CONNECTION_ON_READER);
        LOGGER.error("LongbowReader : Connection closed");
        if (longBowStore != null)
            longBowStore.close();
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

    public LongbowRange getLongbowRange() {
        return longbowRange;
    }

    private void instrumentation(List<Result> scanResult, Instant startTime, Row input) {
        meterStatsManager.markEvent(SUCCESS_ON_READ_DOCUMENT);
        meterStatsManager.updateHistogram(SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        meterStatsManager.updateHistogram(DOCUMENTS_READ_PER_SCAN, scanResult.size());
        if (scanResult.isEmpty() || !Arrays.equals(scanResult.get(0).getRow(), longBowSchema.getKey(input, 0))) {
            meterStatsManager.markEvent(FAILED_TO_READ_LAST_RECORD);
        }
    }

    private List<Result> logException(Throwable ex, Instant startTime) {
        LOGGER.error("LongbowReader : failed to scan document from BigTable: {}", ex.getMessage());
        ex.printStackTrace();
        meterStatsManager.markEvent(FAILED_ON_READ_DOCUMENT);
        errorReporter.reportNonFatalException(new LongbowReaderException(ex));
        meterStatsManager.updateHistogram(FAILED_ON_READ_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        return Collections.emptyList();
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) {
        LOGGER.error("LongbowReader : timeout when reading document");
        meterStatsManager.markEvent(TIMEOUTS_ON_READER);
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
