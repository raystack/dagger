package io.odpf.dagger.core.processors.longbow.storage;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.flink.configuration.Configuration;

import io.odpf.dagger.core.utils.Constants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Result;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

public class LongbowStore {
    private BigtableTableAdminClient adminClient;
    private BigtableAsyncConnection tableClient;
    private Map<String, AsyncTable<AdvancedScanResultConsumer>> tables;

    private LongbowStore(BigtableTableAdminClient adminClient, BigtableAsyncConnection tableClient) {
        this.adminClient = adminClient;
        this.tableClient = tableClient;
        this.tables = new HashMap<>();
    }

    private AsyncTable<AdvancedScanResultConsumer> getTable(String tableId) {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, tableClient.getTable(TableName.valueOf(tableId)));
        }
        return tables.get(tableId);
    }

    public static LongbowStore create(Configuration configuration) throws IOException {
        String gcpProjectID = configuration.getString(Constants.LONGBOW_GCP_PROJECT_ID_KEY, Constants.LONGBOW_GCP_PROJECT_ID_DEFAULT);
        String gcpInstanceID = configuration.getString(Constants.LONGBOW_GCP_INSTANCE_ID_KEY, Constants.LONGBOW_GCP_INSTANCE_ID_DEFAULT);
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
        return getTable(putRequest.getTableId()).put(putRequest.get());
    }

    public CompletableFuture<List<Result>> scanAll(ScanRequest scanRequest) {
        return getTable(scanRequest.getTableId()).scanAll(scanRequest.get());
    }

    public void close() throws IOException {
        if (tableClient != null)
            tableClient.close();
        if (adminClient != null)
            adminClient.close();
    }
}
