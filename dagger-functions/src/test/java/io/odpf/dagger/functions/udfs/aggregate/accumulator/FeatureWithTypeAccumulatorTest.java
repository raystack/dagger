package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import com.google.protobuf.ByteString;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FeatureWithTypeAccumulatorTest {
    @Test
    public void shouldHandleAllValueTypesWithFeatureTypeUdf() {
        FeatureWithTypeAccumulator featureAccumulator = new FeatureWithTypeAccumulator();
        HashMap<String, Tuple2<Object, ValueEnum>> data = new HashMap<>();
        data.put("FloatKey", new Tuple2<>(1.0F, FloatType));
        data.put("StringKey", new Tuple2<>("stringValue", StringType));
        data.put("DoubleKey", new Tuple2<>(1.00D, DoubleType));
        data.put("IntegerKey", new Tuple2<>(1, IntegerType));
        data.put("LongKey", new Tuple2<>(1L, LongType));
        data.put("ByteKey", new Tuple2<>(ByteString.copyFrom("value1".getBytes()), ByteType));
        data.put("BoolKey", new Tuple2<>(true, BooleanType));
        data.put("TimestampKey", new Tuple2<>(getTimestampAsRow(123141, 431231), TimestampType));
        populateFeatureAccumulator(featureAccumulator, data);

        Row[] features = featureAccumulator.getFeaturesAsRows();

        validateFeatures(data, features);
    }


    @Test
    public void shouldPopulateIdAndNameWithSameValuesInFeatureRowWithFeatureTypeUdf() {
        FeatureWithTypeAccumulator featureAccumulator = new FeatureWithTypeAccumulator();
        featureAccumulator.add("FloatKey", 1.0F, FloatType);

        Row[] features = featureAccumulator.getFeaturesAsRows();

        assertEquals(1, features.length);
        assertEquals("FloatKey", features[0].getField(0));
        assertEquals("FloatKey", features[0].getField(2));
    }

    @Test
    public void shouldPopulateAndRemoveKeysInFeatureRowWithFeatureTypeUdf() {
        FeatureWithTypeAccumulator featureAccumulator = new FeatureWithTypeAccumulator();
        featureAccumulator.add("FloatKey", 1.0F, FloatType);
        featureAccumulator.add("keyToRemove", 1.0F, FloatType);
        featureAccumulator.remove("keyToRemove", 1.0F, FloatType);

        Row[] features = featureAccumulator.getFeaturesAsRows();

        assertEquals(1, features.length);
        assertEquals("FloatKey", features[0].getField(0));
        assertEquals("FloatKey", features[0].getField(2));
    }

    @Test
    public void shouldPopulateDuplicateKeysWithDifferentValuesInFeatureRowWithFeatureTypeUdf() {
        FeatureWithTypeAccumulator featureAccumulator = new FeatureWithTypeAccumulator();
        HashMap<String, Tuple2<Object, ValueEnum>> data = new HashMap<>();
        data.put("duplicateKey", new Tuple2<>(1.0F, FloatType));
        data.put("duplicateKey", new Tuple2<>(2.0F, FloatType));
        populateFeatureAccumulator(featureAccumulator, data);

        Row[] features = featureAccumulator.getFeaturesAsRows();

        validateFeatures(data, features);
    }

    @Test
    public void shouldNotPopulateDuplicateKeysWithSameValuesAndTypeInFeatureRowWithFeatureTypeUdf() {
        FeatureWithTypeAccumulator featureAccumulator = new FeatureWithTypeAccumulator();
        featureAccumulator.add("FloatKey", 1.0F, FloatType);
        featureAccumulator.add("FloatKey", 1.0F, FloatType);

        Row[] features = featureAccumulator.getFeaturesAsRows();

        assertEquals(1, features.length);
        assertEquals("FloatKey", features[0].getField(0));
        assertEquals("FloatKey", features[0].getField(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForValuesItCannotHandleWithFeatureTypeUdf() {
        FeatureWithTypeAccumulator featureAccumulator = new FeatureWithTypeAccumulator();
        featureAccumulator.add("key1", "value".getBytes(), null);
        featureAccumulator.getFeaturesAsRows();
    }

    private void validateFeatures(HashMap<String, Tuple2<Object, ValueEnum>> data, Row[] features) {
        assertEquals(data.size(), features.length);
        HashMap<String, Boolean> uniqueKeysPresentInFeatures = new HashMap<>();
        for (Row feature : features) {
            String key = (String) feature.getField(0);
            uniqueKeysPresentInFeatures.put(key, true);
            assertTrue(String.format("%s was not added in the data to create features", key), data.containsKey(key));
            assertEquals(String.format("%s data didn't match", key), data.get(key).f0, ((Row) feature.getField(1)).getField(data.get(key).f1.getValue()));
        }
        assertEquals(data.size(), uniqueKeysPresentInFeatures.size());
    }

    private void populateFeatureAccumulator(FeatureWithTypeAccumulator featureAccumulator, HashMap<String, Tuple2<Object, ValueEnum>> data) {
        for (Map.Entry<String, Tuple2<Object, ValueEnum>> entry : data.entrySet()) {
            featureAccumulator.add(entry.getKey(), entry.getValue().f0, entry.getValue().f1);
        }
    }

    private Row getTimestampAsRow(int seconds, int nanos) {
        Row timestamp1 = new Row(2);
        timestamp1.setField(0, seconds);
        timestamp1.setField(1, nanos);
        return timestamp1;
    }
}
