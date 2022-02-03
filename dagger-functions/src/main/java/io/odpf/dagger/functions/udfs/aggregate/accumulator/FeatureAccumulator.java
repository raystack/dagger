package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import io.odpf.dagger.functions.udfs.aggregate.feast.FeatureUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The accumulator for Feature udf.
 */
public class FeatureAccumulator implements Serializable {

    private @DataTypeHint("RAW") List<Tuple2<String, Object>> features = new ArrayList<>();
    private static final Integer FEATURE_ROW_LENGTH = 3;

    /**
     * Add features.
     *
     * @param key   the key
     * @param value the value
     */
    public void add(String key, Object value) {
        features.add(new Tuple2<>(key, value));
    }

    /**
     * Get features rows.
     *
     * @return the rows
     */
    public Row[] getFeaturesAsRows() {
        ArrayList<Row> featureRows = new ArrayList<>();
        for (Tuple2<String, Object> feature : features) {
            String key = feature.f0;
            Object value = feature.f1;
            FeatureUtils.populateFeatures(featureRows, key, value, FEATURE_ROW_LENGTH);
        }
        return featureRows.toArray(new Row[0]);
    }

    public List<Tuple2<String, Object>> getFeatures() {
        return features;
    }

    public void setFeatures(List<Tuple2<String, Object>> features) {
        this.features = features;
    }
}
