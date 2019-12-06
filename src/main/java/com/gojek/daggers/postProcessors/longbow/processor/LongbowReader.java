package com.gojek.daggers.postProcessors.longbow.processor;

import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.TelemetryPublisher;
import com.gojek.daggers.metrics.aspects.LongbowReaderAspects;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import com.gojek.daggers.postProcessors.longbow.LongbowStore;
import com.gojek.daggers.postProcessors.longbow.row.LongbowRow;
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
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.gojek.daggers.metrics.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.metrics.aspects.LongbowReaderAspects.*;
import static com.gojek.daggers.utils.Constants.*;
import static java.time.Duration.between;

public class LongbowReader extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);
    private static final Logger LOGGER = LoggerFactory.getLogger(LongbowReader.class.getName());
    private Configuration configuration;
    private LongbowSchema longBowSchema;
    private LongbowRow longbowRow;
    private LongbowStore longBowStore;
    private MeterStatsManager meterStatsManager;
    private Map<String, List<String>> metrics = new HashMap<>();

    LongbowReader(Configuration configuration, LongbowSchema longBowSchema, LongbowRow longbowRow, LongbowStore longBowStore, MeterStatsManager meterStatsManager) {
        this(configuration, longBowSchema, longbowRow);
        this.longBowStore = longBowStore;
        this.meterStatsManager = meterStatsManager;
    }

    public LongbowReader(Configuration configuration, LongbowSchema longBowSchema, LongbowRow longbowRow) {
        this.configuration = configuration;
        this.longBowSchema = longBowSchema;
        this.longbowRow = longbowRow;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (longBowStore == null)
            longBowStore = LongbowStore.create(configuration);
        if (meterStatsManager == null)
            meterStatsManager = new MeterStatsManager(getRuntimeContext(), true);
        meterStatsManager.register("longbow.reader", LongbowReaderAspects.values());
        longBowStore.initialize();
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
        Scan scanRequest = new Scan();
        scanRequest.withStartRow(longbowRow.getLatest(input), true);
        scanRequest.withStopRow(longbowRow.getEarliest(input), true);
        longBowSchema
                .getColumnNames(this::isLongbowData)
                .forEach(column -> scanRequest.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column)));
        Instant startTime = Instant.now();
        longBowStore
                .scanAll(scanRequest)
                .exceptionally(throwable -> logException(throwable, startTime))
                .thenAccept(scanResult -> populateResult(scanResult, input, resultFuture, startTime));
    }

    public LongbowRow getLongbowRow() {
        return longbowRow;
    }

    private void populateResult(List<Result> scanResult, Row input, ResultFuture<Row> resultFuture, Instant startTime) {
        meterStatsManager.markEvent(SUCCESS_ON_READ_DOCUMENT);
        meterStatsManager.updateHistogram(SUCCESS_ON_READ_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        meterStatsManager.updateHistogram(DOCUMENTS_READ_PER_SCAN, scanResult.size());
        resultFuture.complete(getRow(scanResult, input));
    }

    private List<Result> logException(Throwable ex, Instant startTime) {
        LOGGER.error("LongbowReader : failed to scan document from BigTable: {}", ex.getMessage());
        ex.printStackTrace();
        meterStatsManager.markEvent(FAILED_ON_READ_DOCUMENT);
        meterStatsManager.updateHistogram(FAILED_ON_READ_DOCUMENT_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        return Collections.emptyList();
    }

    private Collection<Row> getRow(List<Result> scanResult, Row input) {
        Row output = new Row(longBowSchema.getColumnSize());
        HashMap<String, Object> columnMap = new HashMap<>();
        longBowSchema
                .getColumnNames(this::isLongbowData)
                .forEach(name -> columnMap.put(name, getLongbowData(scanResult, name, input)));
        longBowSchema
                .getColumnNames(c -> !isLongbowData(c))
                .forEach(name -> columnMap.put(name, longBowSchema.getValue(input, name)));

        columnMap.forEach((name, data) -> output.setField(longBowSchema.getIndex(name), data));
        return Collections.singletonList(output);
    }

    private List<String> getLongbowData(List<Result> resultScan, String column, Row input) {
        if (resultScan.isEmpty() || !Arrays.equals(resultScan.get(0).getRow(), longBowSchema.getKey(input, 0))) {
            LOGGER.error("LongbowReader : Last record not received");
            meterStatsManager.markEvent(FAILED_TO_READ_LAST_RECORD);
        }
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
        meterStatsManager.markEvent(TIMEOUTS_ON_READER);
        resultFuture.completeExceptionally(
                new TimeoutException("Async function call has timed out."));
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
