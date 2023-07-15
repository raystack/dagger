package org.raystack.dagger.functions.udfs.scalar;

import org.raystack.dagger.common.metrics.managers.MeterStatsManager;
import org.raystack.dagger.common.udfs.ScalarUdf;
import org.raystack.dagger.functions.exceptions.KeyDoesNotExistException;
import org.raystack.dagger.functions.udfs.scalar.dart.DartAspects;
import org.raystack.dagger.functions.udfs.scalar.dart.store.gcs.GcsDataStore;
import org.raystack.dagger.functions.udfs.scalar.dart.types.MapCache;
import org.apache.flink.table.functions.FunctionContext;

import java.util.HashMap;
import java.util.Map;

import static org.raystack.dagger.common.core.Constants.UDF_TELEMETRY_GROUP_KEY;

/**
 * The DartGet udf.
 */
public class DartGet extends ScalarUdf {
    private final GcsDataStore dataStore;
    private final Map<String, MapCache> cache;

    /**
     * Instantiates a new Dart get.
     *
     * @param dataStore the data store
     */
    public DartGet(GcsDataStore dataStore) {
        this.dataStore = dataStore;
        cache = new HashMap<>();
    }

    /**
     * With gcs data store dart get.
     *
     * @param projectId the project id
     * @param bucketId  the bucket id
     * @return the dart get
     */
    public static DartGet withGcsDataStore(String projectId, String bucketId) {
        return new DartGet(new GcsDataStore(projectId, bucketId));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        MeterStatsManager meterStatsManager = new MeterStatsManager(context.getMetricGroup(), true);
        meterStatsManager.register(UDF_TELEMETRY_GROUP_KEY, this.getName(), DartAspects.values());
        dataStore.setMeterStatsManager(meterStatsManager);
        dataStore.setGaugeStatsManager(getGaugeStatsManager());
    }

    /**
     * To fetch a corresponding value in a collection given a key from data point.
     *
     * @param collectionName     the collection name
     * @param key                the key
     * @param refreshRateInHours ttl
     * @return the value in string
     * @author gaurav.s
     * @team DE
     */
    public String eval(String collectionName, String key, Integer refreshRateInHours) {
        if (cache.isEmpty() || !cache.containsKey(collectionName) || cache.get(collectionName).hasExpired(refreshRateInHours) || cache.get(collectionName).isEmpty()) {
            cache.put(collectionName, dataStore.getMap(collectionName));
            dataStore.getMeterStatsManager().markEvent(DartAspects.DART_GCS_FETCH_SUCCESS);
        }
        dataStore.getMeterStatsManager().markEvent(DartAspects.DART_CACHE_HIT);
        return cache.get(collectionName).get(key);
    }

    /**
     * Corresponding value in a GCS bucket given a key from data point.
     *
     * @param collectionName     the collection name
     * @param key                the key
     * @param refreshRateInHours the refresh rate in hours
     * @param defaultValue       the default value
     * @return the string
     */
    public String eval(String collectionName, String key, Integer refreshRateInHours, String defaultValue) {
        try {
            return eval(collectionName, key, refreshRateInHours);
        } catch (KeyDoesNotExistException e) {
            dataStore.getMeterStatsManager().markEvent(DartAspects.DART_CACHE_MISS);
            return defaultValue;
        }
    }

}
