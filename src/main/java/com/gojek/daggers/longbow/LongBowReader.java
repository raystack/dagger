package com.gojek.daggers.longbow;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.gojek.daggers.Constants.*;

public class LongBowReader extends RichAsyncFunction<Row, Row> {

    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);
    private String projectID;
    private String instanceID;
    private LongBowSchema longBowSchema;
    private AsyncTable<AdvancedScanResultConsumer> asyncTable;
    private BigtableAsyncConnection bigtableAsyncConnection;
    private String daggerName;

    LongBowReader(Configuration configuration, LongBowSchema longBowSchema, BigtableAsyncConnection bigtableAsyncConnection) {
        this(configuration, longBowSchema);
        this.bigtableAsyncConnection = bigtableAsyncConnection;
    }


    public LongBowReader(Configuration configuration, LongBowSchema longBowSchema) {
        this.projectID = configuration.getString(LONGBOW_GCP_PROJECT_ID_KEY, LONGBOW_GCP_PROJECT_ID_DEFAULT);
        this.instanceID = configuration.getString(LONGBOW_GCP_INSTANCE_ID_KEY, LONGBOW_GCP_INSTANCE_ID_DEFAULT);
        this.daggerName = configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT);
        this.longBowSchema = longBowSchema;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        org.apache.hadoop.conf.Configuration bigTableConfiguration = BigtableConfiguration.configure(projectID, instanceID);
        if (bigtableAsyncConnection == null)
            bigtableAsyncConnection = new BigtableAsyncConnection(bigTableConfiguration);
        TableName tableName = TableName.valueOf(Bytes.toBytes(daggerName));
        asyncTable = bigtableAsyncConnection.getTable(tableName);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (bigtableAsyncConnection != null)
            bigtableAsyncConnection.close();
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        Scan scanRequest = new Scan();
        long longBowDurationOffset = longBowSchema.getDurationInMillis(input);
        scanRequest.withStartRow(longBowSchema.getKey(input, 0), true);
        scanRequest.withStopRow(longBowSchema.getKey(input, longBowDurationOffset), true);
        longBowSchema
                .getColumns(this::isData)
                .forEach(column -> scanRequest.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column)));
        CompletableFuture<List<Result>> scanFuture = asyncTable.scanAll(scanRequest);
        scanFuture.thenAccept(scanResult -> resultFuture.complete(getRow((List<Result>) scanResult, input)));
    }

    private Collection<Row> getRow(List<Result> scanResult, Row input) {
        Row output = new Row(longBowSchema.getColumnSize());
        HashMap<String, Object> generatedDataMap = new HashMap<>();
        HashMap<String, Object> sqlDataMap = new HashMap<>();
        longBowSchema
                .getColumns(this::isData)
                .forEach(column -> generatedDataMap.put(column, parseColumnData(scanResult, column)));
        longBowSchema
                .getColumns(c -> !isData(c))
                .forEach(s -> sqlDataMap.put(s, input.getField(longBowSchema.getIndex(s))));
        generatedDataMap.forEach((key, value) -> output.setField(longBowSchema.getIndex(key), value));
        sqlDataMap.forEach((key, value) -> output.setField(longBowSchema.getIndex(key), value));

        return Collections.singletonList(output);
    }

    private List<String> parseColumnData(List<Result> resultScan, String column) {
        return resultScan
                .stream()
                .map(result -> Bytes.toString(result.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(column))))
                .collect(Collectors.toList());
    }

    private boolean isData(Map.Entry<String, Integer> c) {
        return c.getKey().contains(LONGBOW_DATA);
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {

    }
}
