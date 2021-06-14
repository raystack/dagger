package io.odpf.dagger.functions.udfs.scalar.dart.store.gcs;

import io.odpf.dagger.common.metrics.managers.GaugeStatsManager;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.functions.udfs.scalar.DartContains;
import io.odpf.dagger.functions.udfs.scalar.DartGet;
import io.odpf.dagger.functions.udfs.scalar.dart.DartAspects;
import io.odpf.dagger.functions.udfs.scalar.dart.store.DataStore;
import io.odpf.dagger.functions.udfs.scalar.dart.types.MapCache;
import io.odpf.dagger.functions.udfs.scalar.dart.types.SetCache;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * The type Gcs data store.
 */
public class GcsDataStore implements DataStore, Serializable {

    private final String projectId;

    private final String bucketId;

    private GcsClient gcsClient;

    private MeterStatsManager meterStatsManager;
    private GaugeStatsManager gaugeStatsManager;

    /**
     * Instantiates a new Gcs data store.
     *
     * @param projectId the project id
     * @param bucketId  the bucket id
     */
    public GcsDataStore(String projectId, String bucketId) {
        this.projectId = projectId;
        this.bucketId = bucketId;
    }

    @Override
    public SetCache getSet(String setName) {

        return new SetCache(getSetOfObjects(setName));
    }

    @Override
    public MapCache getMap(String mapName) {
        Map<String, String> mapOfObjects = getMapOfObjects(mapName);
        return new MapCache(mapOfObjects);
    }

    /**
     * Sets meter stats manager.
     *
     * @param meterStatsManager the meter stats manager
     */
    public void setMeterStatsManager(MeterStatsManager meterStatsManager) {
        this.meterStatsManager = meterStatsManager;
    }

    /**
     * Gets meter stats manager.
     *
     * @return the meter stats manager
     */
    public MeterStatsManager getMeterStatsManager() {
        return this.meterStatsManager;
    }

    /**
     * Sets gauge stats manager.
     *
     * @param gaugeStatsManager the gauge stats manager
     */
    public void setGaugeStatsManager(GaugeStatsManager gaugeStatsManager) {
        this.gaugeStatsManager = gaugeStatsManager;
    }

    /**
     * Gets gauge stats manager.
     *
     * @return the gauge stats manager
     */
    public GaugeStatsManager getGaugeStatsManager() {
        return gaugeStatsManager;
    }

    private Map<String, String> getMapOfObjects(String dartName) {

        String jsonData = getGcsClient().fetchJsonData(DartGet.class.getSimpleName(), getGaugeStatsManager(), this.bucketId, "dart-get/" + dartName);

        ObjectMapper mapper = new ObjectMapper();

        Map<String, String> map = null;
        try {
            map = mapper.readValue(jsonData, Map.class);
        } catch (IOException e) {
            getMeterStatsManager().markEvent(DartAspects.DART_GCS_FETCH_FAILURES);
            e.printStackTrace();
        }
        return map;
    }

    private Set<String> getSetOfObjects(String dartName) {

        String jsonData = getGcsClient().fetchJsonData(DartContains.class.getSimpleName(), getGaugeStatsManager(), this.bucketId, "dart-contains/" + dartName);
        ObjectMapper mapper = new ObjectMapper();
        try {
            ObjectNode node = (ObjectNode) mapper.readTree(jsonData);
            JsonNode arrayNode = node.get("data");
            List<String> list = mapper.readValue(arrayNode.traverse(),
                    new TypeReference<ArrayList<String>>() {
                    });

            return new HashSet<>(list);
        } catch (Exception e) {
            getMeterStatsManager().markEvent(DartAspects.DART_GCS_FETCH_FAILURES);
            e.printStackTrace();
        }

        return new HashSet<>();
    }

    /**
     * Gets gcs client.
     *
     * @return the gcs client
     */
    GcsClient getGcsClient() {
        if (this.gcsClient == null) {
            this.gcsClient = new GcsClient(this.projectId);
        }
        return this.gcsClient;
    }
}
