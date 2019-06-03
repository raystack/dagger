package com.gojek.daggers.longbow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gojek.daggers.Constants.LONGBOW_COLUMN_FAMILY_DEFAULT;
import static com.gojek.daggers.Constants.LONGBOW_DATA;

public class LongBowReader extends RichAsyncFunction<Row, Row> {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);
    private static final Logger LOGGER = LoggerFactory.getLogger(LongBowReader.class.getName());
    private Configuration configuration;
    private LongBowSchema longBowSchema;
    private LongBowStore longBowStore;

    LongBowReader(Configuration configuration, LongBowSchema longBowSchema, LongBowStore longBowStore) {
        this(configuration, longBowSchema);
        this.longBowStore = longBowStore;
    }

    public LongBowReader(Configuration configuration, LongBowSchema longBowSchema) {
        this.configuration = configuration;
        this.longBowSchema = longBowSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (longBowStore == null) {
            longBowStore = LongBowStore.create(configuration);
        }
        longBowStore.initialize();
    }

    @Override
    public void close() throws Exception {
        super.close();
        longBowStore.close();
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        Scan scanRequest = new Scan();
        long longBowDurationOffset = longBowSchema.getDurationInMillis(input);
        scanRequest.withStartRow(longBowSchema.getKey(input, 0), true);
        scanRequest.withStopRow(longBowSchema.getKey(input, longBowDurationOffset), true);
        longBowSchema
                .getColumnNames(this::isLongbowData)
                .forEach(column -> scanRequest.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column)));
        longBowStore
                .scanAll(scanRequest)
                .exceptionally(this::logException)
                .thenAccept(scanResult -> resultFuture.complete(getRow((List<Result>) scanResult, input)));
    }

    private List<Result> logException(Throwable ex) {
        LOGGER.error("LongBowReader : failed to scan document from BigTable: {}", ex.getMessage());
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
        resultFuture.complete(Collections.emptyList());
    }
}
