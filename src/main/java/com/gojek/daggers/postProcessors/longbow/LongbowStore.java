package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.postProcessors.longbow.storage.PutRequest;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.gojek.daggers.utils.Constants.*;
import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

public class LongbowStore {
    private BigtableTableAdminClient adminClient;
    private BigtableAsyncConnection tableClient;
    private String daggerID;
    private AsyncTable<AdvancedScanResultConsumer> table;

    private LongbowStore(BigtableTableAdminClient adminClient, BigtableAsyncConnection tableClient, String daggerID) {
        this.adminClient = adminClient;
        this.tableClient = tableClient;
        this.daggerID = daggerID;
    }

    public static LongbowStore create(Configuration configuration) throws IOException {
        String gcpProjectID = configuration.getString(LONGBOW_GCP_PROJECT_ID_KEY, LONGBOW_GCP_PROJECT_ID_DEFAULT);
        String gcpInstanceID = configuration.getString(LONGBOW_GCP_INSTANCE_ID_KEY, LONGBOW_GCP_INSTANCE_ID_DEFAULT);
        String daggerID = configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT);
        BigtableTableAdminClient bigtableTableAdminClient = BigtableTableAdminClient.create(gcpProjectID, gcpInstanceID);
        org.apache.hadoop.conf.Configuration bigTableConfiguration = BigtableConfiguration.configure(gcpProjectID, gcpInstanceID);
        BigtableAsyncConnection bigtableAsyncConnection = new BigtableAsyncConnection(bigTableConfiguration);
        return new LongbowStore(bigtableTableAdminClient, bigtableAsyncConnection, daggerID);
    }

    public boolean tableExists() {
        return adminClient.exists(daggerID);
    }

    public String tableName() {
        return daggerID;
    }

    public void createTable(Duration maxAgeDuration, String columnFamilyName) throws Exception {
        adminClient.createTable(CreateTableRequest.of(daggerID).addFamily(columnFamilyName,
                GCRULES.union()
                        .rule(GCRULES.maxVersions(1))
                        .rule(GCRULES.maxAge(maxAgeDuration))));
    }

    public void initialize() {
        TableName tableName = TableName.valueOf(daggerID);
        table = tableClient.getTable(tableName);
    }

    public CompletableFuture<Void> put(PutRequest putRequest) {
        if (table == null)
            initialize();
        return table.put(putRequest.get());
    }

    public CompletableFuture<List<Result>> scanAll(Scan scanRequest) {
        if (table == null)
            initialize();
        return table.scanAll(scanRequest);
    }

    public void close() throws IOException {
        if (tableClient != null)
            tableClient.close();
        if (adminClient != null)
            adminClient.close();
    }
}
