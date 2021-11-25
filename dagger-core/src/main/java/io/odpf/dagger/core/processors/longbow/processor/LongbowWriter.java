package io.odpf.dagger.core.processors.longbow.processor;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.LongbowWriterAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.processors.longbow.exceptions.LongbowWriterException;
import io.odpf.dagger.core.processors.longbow.outputRow.WriterOutputRow;
import io.odpf.dagger.core.processors.longbow.request.PutRequestFactory;
import io.odpf.dagger.core.processors.longbow.storage.LongbowStore;
import io.odpf.dagger.core.processors.longbow.storage.PutRequest;
import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static java.time.Duration.between;

/**
 * The Longbow writer.
 */
public class LongbowWriter extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(LongbowWriter.class.getName());
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(Constants.LONGBOW_COLUMN_FAMILY_DEFAULT);

    private MeterStatsManager meterStatsManager;
    private LongbowSchema longbowSchema;
    private Configuration configuration;
    private String longbowDocumentDuration;
    private PutRequestFactory putRequestFactory;
    private String tableId;
    private WriterOutputRow writerOutputRow;
    private LongbowStore longBowStore;
    private Map<String, List<String>> metrics = new HashMap<>();
    private ErrorReporter errorReporter;

    /**
     * Instantiates a new Longbow writer.
     *
     * @param configuration     the configuration
     * @param longbowSchema     the longbow schema
     * @param putRequestFactory the put request factory
     * @param tableId           the table id
     * @param writerOutputRow   the writer output row
     */
    public LongbowWriter(Configuration configuration, LongbowSchema longbowSchema, PutRequestFactory putRequestFactory, String tableId, WriterOutputRow writerOutputRow) {

        this.longbowSchema = longbowSchema;
        this.longbowDocumentDuration = configuration.getString(Constants.PROCESSOR_LONGBOW_DOCUMENT_DURATION_KEY,
                Constants.PROCESSOR_LONGBOW_DOCUMENT_DURATION_DEFAULT);
        this.putRequestFactory = putRequestFactory;
        this.tableId = tableId;
        this.writerOutputRow = writerOutputRow;
        this.configuration = configuration;
    }

    /**
     * Instantiates a new Longbow writer with specified longbow store.
     *
     * @param configuration     the configuration
     * @param longBowSchema     the longbow schema
     * @param meterStatsManager the meter stats manager
     * @param errorReporter     the error reporter
     * @param longBowStore      the longbow store
     * @param putRequestFactory the put request factory
     * @param tableId           the table id
     * @param writerOutputRow   the writer output row
     */
    LongbowWriter(Configuration configuration, LongbowSchema longBowSchema, MeterStatsManager meterStatsManager,
                  ErrorReporter errorReporter, LongbowStore longBowStore, PutRequestFactory putRequestFactory, String tableId, WriterOutputRow writerOutputRow) {
        this(configuration, longBowSchema, putRequestFactory, tableId, writerOutputRow);
        this.meterStatsManager = meterStatsManager;
        this.longBowStore = longBowStore;
        this.errorReporter = errorReporter;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration internalFlinkConfig) throws Exception {
        super.open(internalFlinkConfig);
        if (longBowStore == null) {
            longBowStore = LongbowStore.create(this.configuration);
        }

        if (meterStatsManager == null) {
            meterStatsManager = new MeterStatsManager(getRuntimeContext().getMetricGroup(), true);
        }
        meterStatsManager.register("longbow.writer", LongbowWriterAspects.values());

        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext().getMetricGroup(), configuration);
        }

        if (!longBowStore.tableExists(tableId)) {
            Instant startTime = Instant.now();
            try {
                Duration maxAgeDuration = Duration.ofMillis(longbowSchema.getDurationInMillis(longbowDocumentDuration));
                String columnFamilyName = new String(COLUMN_FAMILY_NAME);
                longBowStore.createTable(maxAgeDuration, columnFamilyName, tableId);
                LOGGER.info("table '{}' is created with maxAge '{}' on column family '{}'", tableId,
                        maxAgeDuration, columnFamilyName);
                meterStatsManager.markEvent(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE);
                meterStatsManager.updateHistogram(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME,
                        between(startTime, Instant.now()).toMillis());
            } catch (Exception ex) {
                LOGGER.error("failed to create table '{}'", tableId);
                meterStatsManager.markEvent(LongbowWriterAspects.FAILURES_ON_CREATE_BIGTABLE);
                errorReporter.reportFatalException(ex);
                meterStatsManager.updateHistogram(LongbowWriterAspects.FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME,
                        between(startTime, Instant.now()).toMillis());
                throw ex;
            }
        }
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(TelemetryTypes.POST_PROCESSOR_TYPE.getValue(), Constants.LONGBOW_WRITER_PROCESSOR_KEY);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        PutRequest putRequest = putRequestFactory.create(input);
        Instant startTime = Instant.now();
        CompletableFuture<Void> writeFuture = longBowStore.put(putRequest);
        writeFuture.exceptionally(throwable -> logException(throwable, startTime)).thenAccept(aVoid -> {
            meterStatsManager.markEvent(LongbowWriterAspects.SUCCESS_ON_WRITE_DOCUMENT);
            meterStatsManager.updateHistogram(LongbowWriterAspects.SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME,
                    between(startTime, Instant.now()).toMillis());
            resultFuture.complete(Collections.singletonList(writerOutputRow.get(input)));
        });
    }

    private Void logException(Throwable ex, Instant startTime) {
        LOGGER.error("failed to write document to table '{}'", tableId);
        ex.printStackTrace();
        meterStatsManager.markEvent(LongbowWriterAspects.FAILED_ON_WRITE_DOCUMENT);
        errorReporter.reportNonFatalException(new LongbowWriterException(ex));
        meterStatsManager.updateHistogram(LongbowWriterAspects.FAILED_ON_WRITE_DOCUMENT_RESPONSE_TIME,
                between(startTime, Instant.now()).toMillis());
        return null;
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        LOGGER.error("LongbowWriter : timeout when writing document");
        meterStatsManager.markEvent(LongbowWriterAspects.TIMEOUTS_ON_WRITER);
        Exception timeoutException = new TimeoutException("Async function call has timed out.");
        errorReporter.reportFatalException(timeoutException);
        resultFuture.completeExceptionally(timeoutException);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (longBowStore != null) {
            longBowStore.close();
        }
        meterStatsManager.markEvent(LongbowWriterAspects.CLOSE_CONNECTION_ON_WRITER);
        LOGGER.error("LongbowWriter : Connection closed");
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
