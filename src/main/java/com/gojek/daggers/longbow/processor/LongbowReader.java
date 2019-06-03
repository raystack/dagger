package com.gojek.daggers.longbow.processor;

import com.gojek.daggers.longbow.LongbowSchema;
import com.gojek.daggers.longbow.LongbowStore;
import com.gojek.daggers.longbow.metric.LongbowAspects;
import com.gojek.daggers.utils.stats.StatsManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gojek.daggers.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static com.gojek.daggers.Constants.LONGBOW_DATA;
import static java.time.Duration.between;

public class LongbowReader extends RichAsyncFunction<Row, Row> {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);
    private static final Logger LOGGER = LoggerFactory.getLogger(LongbowReader.class.getName());
    private Configuration configuration;
    private LongbowSchema longBowSchema;
    private LongbowStore longBowStore;
    private StatsManager statsManager;

    LongbowReader(Configuration configuration, LongbowSchema longBowSchema, LongbowStore longBowStore, StatsManager statsManager) {
        this(configuration, longBowSchema);
        this.longBowStore = longBowStore;
        this.statsManager = statsManager;
    }

    public LongbowReader(Configuration configuration, LongbowSchema longBowSchema) {
        this.configuration = configuration;
        this.longBowSchema = longBowSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (longBowStore == null)
            longBowStore = LongbowStore.create(configuration);
        if(statsManager == null)
            statsManager = new StatsManager(getRuntimeContext(), true);
        statsManager.register(longBowStore.groupName(), LongbowAspects.values());
        longBowStore.initialize();
    }

    @Override
    public void close() throws Exception {
        super.close();
        statsManager.markEvent(LongbowAspects.CLOSE_CONNECTION_ON_READER);
        LOGGER.error("LongbowReader : Connection closed");
        longBowStore.close();
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        Scan scanRequest = new Scan();
        long longBowDurationOffset = longBowSchema.getDurationInMillis(input);
        scanRequest.withStartRow(longBowSchema.getKey(input, 0), true);
        scanRequest.withStopRow(longBowSchema.getKey(input, longBowDurationOffset), true);
        Instant startTime = Instant.now();
        longBowSchema
                .getColumnNames(this::isLongbowData)
                .forEach(column -> scanRequest.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column)));
        longBowStore
                .scanAll(scanRequest)
                .exceptionally(throwable -> logException(throwable, startTime))
                .thenAccept(scanResult -> populateResult(scanResult, input, resultFuture, startTime));
    }

    private void populateResult(List<Result> scanResult, Row input, ResultFuture<Row> resultFuture, Instant startTime) {
        statsManager.markEvent(LongbowAspects.SUCCESS_ON_READ_DOCUMENT);
        statsManager.updateHistogram(LongbowAspects.SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        resultFuture.complete(getRow(scanResult, input));
    }

    private List<Result> logException(Throwable ex, Instant startTime) {
        LOGGER.error("LongbowReader : failed to scan document from BigTable: {}", ex.getMessage());
        ex.printStackTrace();
        statsManager.markEvent(LongbowAspects.FAILURES_ON_READ_DOCUMENT);
        statsManager.updateHistogram(LongbowAspects.FAILURES_ON_READ_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        return Collections.emptyList();
    }

    private Collection<Row> getRow(List<Result> scanResult, Row input) {
        Row output = new Row(longBowSchema.getColumnSize());
        HashMap<String, Object> columnMap = new HashMap<>();
        longBowSchema
                .getColumnNames(this::isLongbowData)
                .forEach(name -> columnMap.put(name, getLongbowData(scanResult, name)));
        longBowSchema
                .getColumnNames(c -> !isLongbowData(c))
                .forEach(name -> columnMap.put(name, input.getField(longBowSchema.getIndex(name))));
        columnMap.forEach((name, data) -> output.setField(longBowSchema.getIndex(name), data));
        return Collections.singletonList(output);
    }

    private List<String> getLongbowData(List<Result> resultScan, String column) {
        return resultScan
                .stream()
                .map(result -> Bytes.toString(result.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(column))))
                .collect(Collectors.toList());
    }

    private boolean isLongbowData(Map.Entry<String, Integer> c) {
        return c.getKey().contains(LONGBOW_DATA);
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        LOGGER.error("LongbowReader : timeout when reading document");
        statsManager.markEvent(LongbowAspects.TIMEOUTS_ON_READER);
        resultFuture.complete(Collections.emptyList());
    }
}
