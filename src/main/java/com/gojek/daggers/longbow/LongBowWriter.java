package com.gojek.daggers.longbow;

import com.gojek.daggers.async.metric.Aspects;
import com.gojek.daggers.async.metric.StatsManager;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.gojek.daggers.Constants.*;
import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

public class LongBowWriter extends RichAsyncFunction<Row, Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LongBowWriter.class.getName());
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes(LONGBOW_COLUMN_FAMILY_DEFAULT);

    private StatsManager statsManager;
    private LongBowSchema longbowSchema;
    private String gcpProjectID;
    private String gcpInstanceID;
    private String daggerID;
    private String longbowDocumentDuration;
    private BigtableAsyncConnection bigtableClient;
    // client to create table and set GCRULES (maxAge & maxVersions)
    private BigtableTableAdminClient bigtableAdminClient;

    LongBowWriter(Configuration configuration, LongBowSchema longbowSchema, BigtableAsyncConnection bigtableClient, BigtableTableAdminClient bigtableTableAdminClient) {
        this(configuration, longbowSchema);
        this.bigtableClient = bigtableClient;
        this.bigtableAdminClient = bigtableTableAdminClient;
    }

    public LongBowWriter(Configuration configuration, LongBowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
        this.gcpProjectID = configuration.getString(LONGBOW_GCP_PROJECT_ID_KEY, LONGBOW_GCP_PROJECT_ID_DEFAULT);
        this.gcpInstanceID = configuration.getString(LONGBOW_GCP_INSTANCE_ID_KEY, LONGBOW_GCP_INSTANCE_ID_DEFAULT);
        this.daggerID = configuration.getString(DAGGER_NAME_KEY, DAGGER_NAME_DEFAULT);
        this.longbowDocumentDuration = configuration.getString(LONGBOW_DOCUMENT_DURATION, LONGBOW_DOCUMENT_DURATION_DEFAULT);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        if (bigtableClient == null) {
            org.apache.hadoop.conf.Configuration bigTableConfiguration = BigtableConfiguration.configure(gcpProjectID, gcpInstanceID);
            bigtableClient = new BigtableAsyncConnection(bigTableConfiguration);
        }

        if (bigtableAdminClient == null)
            bigtableAdminClient = BigtableTableAdminClient.create(gcpProjectID, gcpInstanceID);

        String groupName = "longbow." + daggerID;
        statsManager = new StatsManager(getRuntimeContext(), groupName, true);
        statsManager.register();

        Boolean isExist = bigtableAdminClient.exists(daggerID);
        if (!isExist) {
            Duration maxAgeDuration = Duration.ofMillis(longbowSchema.getDurationInMillis(longbowDocumentDuration));
            String columnFamilyName = new String(COLUMN_FAMILY_NAME);
            Table table = bigtableAdminClient.createTable(CreateTableRequest.of(daggerID).addFamily(columnFamilyName,
                    GCRULES.union()
                            .rule(GCRULES.maxVersions(1))               // GC if there are more than 1 version; or
                            .rule(GCRULES.maxAge(maxAgeDuration))));    // GC if the document is expired
            LOGGER.info("LongBowWriter : table '{}' is created with maxAge '{}' on column family '{}'", table.getId(), maxAgeDuration, columnFamilyName);
        }
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        TableName tableName = TableName.valueOf(daggerID);
        AsyncTable<AdvancedScanResultConsumer> asyncTable = bigtableClient.getTable(tableName);

        String longbowKey = (String) input.getField(longbowSchema.getIndex(LONGBOW_KEY));
        Timestamp rowTime = (Timestamp) input.getField(longbowSchema.getIndex((ROWTIME)));
        String longbowRowKey = getLongBowRowKey(longbowKey, rowTime.getTime());

        Put put = new Put(Bytes.toBytes(longbowRowKey));

        longbowSchema
                .getColumns(c -> c.getKey().contains(LONGBOW_DATA))
                .forEach(column -> put.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(column), Bytes.toBytes((String) input.getField(longbowSchema.getIndex(column)))));

        CompletableFuture<Void> writeFuture = asyncTable.put(put);

        writeFuture
                .exceptionally(ex -> {
                    statsManager.markEvent(Aspects.FAILURES_ON_BIGTABLE_WRITE_DOCUMENT);
                    LOGGER.error("LongBowWriter : failed to write document to BigTable: {}", ex.getMessage());
                    return null;
                })
                .thenAccept(aVoid -> resultFuture.complete(Collections.singleton(input)));
    }

    private String getLongBowRowKey(String longbowKey, long unixTimestamp) {
        long reversedTimestamp = Long.MAX_VALUE - unixTimestamp;
        return longbowKey + "#" + reversedTimestamp;
    }

    public void setStatsManager(StatsManager statsManager) {
        this.statsManager = statsManager;
    }
    
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        statsManager.markEvent(Aspects.TIMEOUTS);
        resultFuture.complete(Collections.singleton(input));
    }
}
