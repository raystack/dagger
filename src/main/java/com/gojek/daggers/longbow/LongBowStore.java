package com.gojek.daggers.longbow;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.gojek.daggers.Constants.*;
import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

public class LongBowStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(LongBowStore.class.getName());
    private BigtableTableAdminClient adminClient;
    private BigtableAsyncConnection tableClient;
    private String daggerID;
    private AsyncTable<AdvancedScanResultConsumer> table;

    private LongBowStore(BigtableTableAdminClient adminClient, BigtableAsyncConnection tableClient, String daggerID) {
        this.adminClient = adminClient;
        this.tableClient = tableClient;
        this.daggerID = daggerID;
    }

    public static LongBowStore create(Configuration configuration) throws IOException {
        String gcpProjectID = configuration.getString(LONGBOW_GCP_PROJECT_ID_KEY, LONGBOW_GCP_PROJECT_ID_DEFAULT);
        String gcpInstanceID = configuration.getString(LONGBOW_GCP_INSTANCE_ID_KEY, LONGBOW_GCP_INSTANCE_ID_DEFAULT);
        String daggerID = configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT);
        BigtableTableAdminClient bigtableTableAdminClient = BigtableTableAdminClient.create(gcpProjectID, gcpInstanceID);
        org.apache.hadoop.conf.Configuration bigTableConfiguration = BigtableConfiguration.configure(gcpProjectID, gcpInstanceID);
        BigtableAsyncConnection bigtableAsyncConnection = new BigtableAsyncConnection(bigTableConfiguration);
        return new LongBowStore(bigtableTableAdminClient, bigtableAsyncConnection, daggerID);
    }

    public boolean tableExists() {
        return adminClient.exists(daggerID);
    }

    public String groupName() {
        return "longbow." + daggerID;
    }

    public void createTable(Duration maxAgeDuration, String columnFamilyName) {
        Table table = adminClient.createTable(CreateTableRequest.of(daggerID).addFamily(columnFamilyName,
                GCRULES.union()
                        .rule(GCRULES.maxVersions(1))
                        .rule(GCRULES.maxAge(maxAgeDuration))));

        LOGGER.info("LongBowWriter : table '{}' is created with maxAge '{}' on column family '{}'", table.getId(), maxAgeDuration, columnFamilyName);
    }

    public void initialize() {
        TableName tableName = TableName.valueOf(daggerID);
        table = tableClient.getTable(tableName);
    }

    public CompletableFuture<Void> put(Put putRequest) {
        if (table == null)
            initialize();
        return table.put(putRequest);
    }
}
