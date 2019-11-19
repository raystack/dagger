package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.metrics.LongbowWriterAspects;
import com.gojek.daggers.metrics.StatsManager;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.LongbowStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.utils.Constants.*;
import static java.time.Duration.between;

public class LongbowWriter extends RichAsyncFunction<Row, Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LongbowWriter.class.getName());
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);

    private StatsManager statsManager;
    private Configuration configuration;
    private LongbowSchema longbowSchema;
    private String longbowDocumentDuration;
    private LongbowStore longBowStore;


    public LongbowWriter(Configuration configuration, LongbowSchema longbowSchema) {
        this.configuration = configuration;
        this.longbowSchema = longbowSchema;
        this.longbowDocumentDuration = configuration.getString(LONGBOW_DOCUMENT_DURATION, LONGBOW_DOCUMENT_DURATION_DEFAULT);
    }

    LongbowWriter(Configuration configuration, LongbowSchema longBowSchema, StatsManager statsManager, LongbowStore longBowStore) {
        this(configuration, longBowSchema);
        this.statsManager = statsManager;
        this.longBowStore = longBowStore;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (longBowStore == null)
            longBowStore = LongbowStore.create(configuration);

        if (statsManager == null)
            statsManager = new StatsManager(getRuntimeContext(), true);
        statsManager.register("longbow.writer", LongbowWriterAspects.values());

        if (!longBowStore.tableExists()) {
            Instant startTime = Instant.now();
            try {
                Duration maxAgeDuration = Duration.ofMillis(longbowSchema.getDurationInMillis(longbowDocumentDuration));
                String columnFamilyName = new String(COLUMN_FAMILY_NAME);
                longBowStore.createTable(maxAgeDuration, columnFamilyName);
                LOGGER.info("table '{}' is created with maxAge '{}' on column family '{}'", longBowStore.tableName(), maxAgeDuration, columnFamilyName);
                statsManager.markEvent(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE);
                statsManager.updateHistogram(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
            } catch (Exception ex) {
                LOGGER.error("failed to create table '{}'", longBowStore.tableName());
                statsManager.markEvent(LongbowWriterAspects.FAILURES_ON_CREATE_BIGTABLE);
                statsManager.updateHistogram(LongbowWriterAspects.FAILURES_ON_CREATE_BIGTABLE_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
                throw ex;
            }
        }
        longBowStore.initialize();
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        Put putRequest = new Put(longbowSchema.getKey(input, 0));
        Timestamp rowtime = (Timestamp) longbowSchema.getValue(input, ROWTIME);
        longbowSchema
                .getColumnNames(c -> c.getKey().contains(LONGBOW_DATA))
                .forEach(column -> putRequest
                        .addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column), rowtime.getTime(), Bytes.toBytes((String) longbowSchema.getValue(input, column))));
        Instant startTime = Instant.now();
        CompletableFuture<Void> writeFuture = longBowStore.put(putRequest);
        writeFuture
                .exceptionally(throwable -> logException(throwable, startTime))
                .thenAccept(aVoid -> {
                    statsManager.markEvent(LongbowWriterAspects.SUCCESS_ON_WRITE_DOCUMENT);
                    statsManager.updateHistogram(LongbowWriterAspects.SUCCESS_ON_WRITE_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
                    resultFuture.complete(Collections.singleton(input));
                });
    }

    private Void logException(Throwable ex, Instant startTime) {
        LOGGER.error("failed to write document to table '{}'", longBowStore.tableName());
        ex.printStackTrace();
        statsManager.markEvent(LongbowWriterAspects.FAILED_ON_WRITE_DOCUMENT);
        statsManager.updateHistogram(LongbowWriterAspects.FAILED_ON_WRITE_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        return null;
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        LOGGER.error("LongbowWriter : timeout when writing document");
        statsManager.markEvent(LongbowWriterAspects.TIMEOUTS_ON_WRITER);
        resultFuture.completeExceptionally(
                new TimeoutException("Async function call has timed out."));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (longBowStore != null)
            longBowStore.close();
        statsManager.markEvent(LongbowWriterAspects.CLOSE_CONNECTION_ON_WRITER);
        LOGGER.error("LongbowWriter : Connection closed");
    }
}
