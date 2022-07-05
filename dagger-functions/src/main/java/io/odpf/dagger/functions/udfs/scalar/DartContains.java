package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.dart.DartAspects;
import io.odpf.dagger.functions.udfs.scalar.dart.store.gcs.GcsDataStore;
import io.odpf.dagger.functions.udfs.scalar.dart.types.SetCache;
import org.apache.flink.table.functions.FunctionContext;

import java.util.HashMap;
import java.util.Map;

import static io.odpf.dagger.common.core.Constants.UDF_TELEMETRY_GROUP_KEY;

/**
 * The DartContains udf.
 */
public class DartContains extends ScalarUdf {
    private final GcsDataStore dataStore;
    private final Map<String, SetCache> setCache;

    /**
     * Instantiates a new Dart contains.
     *
     * @param dataStore the data store
     */
    DartContains(GcsDataStore dataStore) {
        this.dataStore = dataStore;
        setCache = new HashMap<>();
    }

    /**
     * With gcs data store dart contains.
     *
     * @param projectId the project id
     * @param bucketId  the bucket id
     * @return the dart contains
     */
    public static DartContains withGcsDataStore(String projectId, String bucketId) {
        return new DartContains(new GcsDataStore(projectId, bucketId));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        MeterStatsManager meterStatsManager = new MeterStatsManager(context.getMetricGroup(), true);
        meterStatsManager.register(UDF_TELEMETRY_GROUP_KEY, this.getName(), DartAspects.values());
        this.dataStore.setMeterStatsManager(meterStatsManager);
        this.dataStore.setGaugeStatsManager(getGaugeStatsManager());

    }

    /**
     * To check if a data point in the message is present in the Redis collection.
     *
     * @param listName the list name
     * @param field    the field
     * @return the boolean
     */
    public boolean eval(String listName, String field) {
        return eval(listName, field, 1);
    }

    /**
     * To check if a data point in the message is present in the Redis collection.
     *
     * @param listName           the list name
     * @param field              the field
     * @param refreshRateInHours ttl
     * @return the boolean
     * @author gaurav.s
     * @team DE
     */
    public boolean eval(String listName, String field, Integer refreshRateInHours) {
        SetCache listData = getListData(listName, field, refreshRateInHours);
        boolean isPresent = listData.contains(field);
        updateMetrics(isPresent);
        return isPresent;
    }

    /**
     * Check if a data point in the message is present in the GCS bucket.
     *
     * @param listName the list name
     * @param field    the field
     * @param regex    the regex
     * @return the boolean
     */
    public boolean eval(String listName, String field, String regex) {
        return eval(listName, field, regex, 1);
    }

    /**
     * Check if a data point in the message is present in the GCS bucket.
     *
     * @param listName           the list name
     * @param field              the field
     * @param regex              the regex
     * @param refreshRateInHours the refresh rate in hours
     * @return the boolean
     */
    public boolean eval(String listName, String field, String regex, Integer refreshRateInHours) {
        SetCache listData = getListData(listName, field, refreshRateInHours);
        boolean isPresent = listData.matches(field, regex);
        updateMetrics(isPresent);
        return isPresent;
    }

    private SetCache getListData(String listName, String field, int refreshRateInHours) {
        if (setCache.isEmpty() || !setCache.containsKey(listName) || setCache.get(listName).hasExpired(refreshRateInHours) || setCache.get(listName).isEmpty()) {
            setCache.put(listName, dataStore.getSet(listName));
            dataStore.getMeterStatsManager().markEvent(DartAspects.DART_GCS_FETCH_SUCCESS);
        }
        return setCache.get(listName);
    }

    private void updateMetrics(boolean isPresent) {
        if (isPresent) {
            dataStore.getMeterStatsManager().markEvent(DartAspects.DART_CACHE_HIT);
        } else {
            dataStore.getMeterStatsManager().markEvent(DartAspects.DART_CACHE_MISS);
        }
    }

}
