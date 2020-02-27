package com.gojek.daggers.postProcessors.longbow.storage;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LongbowTable {
    private AsyncTable<AdvancedScanResultConsumer> table;

    public LongbowTable(String tableId, BigtableAsyncConnection bigtableAsyncConnection) {
        this.table = bigtableAsyncConnection.getTable(TableName.valueOf(tableId));
    }

    public CompletableFuture<Void> put(Put put) {
        return table.put(put);
    }

    public CompletableFuture<List<Result>> scanAll(Scan scan) {
        return table.scanAll(scan);
    }
}
