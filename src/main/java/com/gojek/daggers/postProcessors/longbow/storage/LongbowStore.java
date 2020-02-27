package com.gojek.daggers.postProcessors.longbow.storage;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Result;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.gojek.daggers.utils.Constants.*;
import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

public class LongbowStore {
    private BigtableTableAdminClient adminClient;
    private BigtableAsyncConnection tableClient;
    private Map<String, LongbowTable> tables;

    private LongbowStore(BigtableTableAdminClient adminClient, BigtableAsyncConnection tableClient) {
        this.adminClient = adminClient;
        this.tableClient = tableClient;
        this.tables = new HashMap<>();
    }

    public static LongbowStore create(Configuration configuration) throws IOException {
        String gcpProjectID = configuration.getString(LONGBOW_GCP_PROJECT_ID_KEY, LONGBOW_GCP_PROJECT_ID_DEFAULT);
        String gcpInstanceID = configuration.getString(LONGBOW_GCP_INSTANCE_ID_KEY, LONGBOW_GCP_INSTANCE_ID_DEFAULT);
        BigtableTableAdminClient bigtableTableAdminClient = BigtableTableAdminClient.create(gcpProjectID, gcpInstanceID);
        org.apache.hadoop.conf.Configuration bigTableConfiguration = BigtableConfiguration.configure(gcpProjectID, gcpInstanceID);
        BigtableAsyncConnection bigtableAsyncConnection = new BigtableAsyncConnection(bigTableConfiguration);
        return new LongbowStore(bigtableTableAdminClient, bigtableAsyncConnection);
    }

    public boolean tableExists(String tableId) {
        return adminClient.exists(tableId);
    }

    public void createTable(Duration maxAgeDuration, String columnFamilyName, String tableId) throws Exception {
        adminClient.createTable(CreateTableRequest.of(tableId).addFamily(columnFamilyName,
                GCRULES.union()
                        .rule(GCRULES.maxVersions(1))
                        .rule(GCRULES.maxAge(maxAgeDuration))));
    }

    public CompletableFuture<Void> put(PutRequest putRequest) {
        String tableId = putRequest.getTableId();
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new LongbowTable(tableId, tableClient));
        }
        return tables.get(tableId).put(putRequest.get());
    }

    public CompletableFuture<List<Result>> scanAll(ScanRequest scanRequest) {
        String tableId = scanRequest.getTableId();
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new LongbowTable(tableId, tableClient));
        }
        return tables.get(tableId).scanAll(scanRequest.get());
    }

    public void close() throws IOException {
        if (tableClient != null)
            tableClient.close();
        if (adminClient != null)
            adminClient.close();
    }
}
