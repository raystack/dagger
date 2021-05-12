package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.udfs.scalar.dart.DartAspects;
import io.odpf.dagger.functions.udfs.scalar.dart.store.gcs.GcsDataStore;
import io.odpf.dagger.functions.udfs.scalar.dart.types.SetCache;
import org.apache.flink.table.functions.FunctionContext;

import java.util.HashMap;
import java.util.Map;

public class DartContains extends ScalarUdf {
    private final GcsDataStore dataStore;
    private final Map<String, SetCache> setCache;

    DartContains(GcsDataStore dataStore) {
        this.dataStore = dataStore;
        setCache = new HashMap<>();
    }

    public static DartContains withGcsDataStore(String projectId, String bucketId) {
        return new DartContains(new GcsDataStore(projectId, bucketId));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        MeterStatsManager meterStatsManager = new MeterStatsManager(context.getMetricGroup(), true);
        meterStatsManager.register(this.getClass().getSimpleName(), DartAspects.values());
        this.dataStore.setMeterStatsManager(meterStatsManager);
        this.dataStore.setGaugeStatsManager(getGaugeStatsManager());

    }

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

    public boolean eval(String listName, String field, int refreshRateInHours) {
        SetCache listData = getListData(listName, field, refreshRateInHours);
        boolean isPresent = listData.contains(field);
        updateMetrics(isPresent);
        return isPresent;
    }


    public boolean eval(String listName, String field, String regex) {
        return eval(listName, field, regex, 1);
    }

    public boolean eval(String listName, String field, String regex, int refreshRateInHours) {
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
