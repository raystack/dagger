package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import io.odpf.dagger.functions.udfs.aggregate.feast.FeatureUtils;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * The accumulator for FeatureWithType udf.
 */
public class FeatureWithTypeAccumulator implements Serializable {
    private static final Integer FEATURE_ROW_LENGTH = 3;
    private final HashMap<String, Tuple3<String, Object, ValueEnum>> features = new HashMap<>();

    /**
     * Add features.
     *
     * @param key   the key
     * @param value the value
     * @param type  the type
     */
    public void add(String key, Object value, ValueEnum type) {
        Tuple3<String, Object, ValueEnum> featureTuple = new Tuple3<>(key, value, type);
        features.put(getMapKey(key, featureTuple.hashCode()), featureTuple);
    }

    /**
     * Get features rows.
     *
     * @return the rows
     */
    public Row[] getFeatures() {
        ArrayList<Row> featureRows = new ArrayList<>();
        for (Tuple3<String, Object, ValueEnum> feature : features.values()) {
            String key = feature.f0;
            Object value = feature.f1;
            ValueEnum type = feature.f2;
            FeatureUtils.populateFeaturesWithType(featureRows, key, value, type, FEATURE_ROW_LENGTH);
        }
        return featureRows.toArray(new Row[0]);
    }

    /**
     * Remove features.
     *
     * @param key   the key
     * @param value the value
     * @param type  the type
     */
    public void remove(String key, Object value, ValueEnum type) {
        Tuple3<String, Object, ValueEnum> featureTuple = new Tuple3<>(key, value, type);
        features.remove(getMapKey(key, featureTuple.hashCode()));
    }

    private String getMapKey(String key, Integer hashcode) {
        return String.format("%s-%d", key, hashcode);
    }
}
