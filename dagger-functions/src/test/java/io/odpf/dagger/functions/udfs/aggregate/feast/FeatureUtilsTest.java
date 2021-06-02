package io.odpf.dagger.functions.udfs.aggregate.feast;

import com.google.protobuf.ByteString;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class FeatureUtilsTest {

    @Test
    public void shouldHandleAllValueTypes() {
        ArrayList<Row> featureRows = new ArrayList<>();
        FeatureUtils.populateFeatures(featureRows, "FloatKey", 1.0F, 3);
        FeatureUtils.populateFeatures(featureRows, "StringKey", "stringValue", 3);
        FeatureUtils.populateFeatures(featureRows, "DoubleKey", 1.00D, 3);
        FeatureUtils.populateFeatures(featureRows, "IntegerKey", 1, 3);
        FeatureUtils.populateFeatures(featureRows, "LongKey", 1L, 3);
        FeatureUtils.populateFeatures(featureRows, "ByteKey", ByteString.copyFrom("value1".getBytes()), 3);
        FeatureUtils.populateFeatures(featureRows, "BoolKey", true, 3);
        FeatureUtils.populateFeatures(featureRows, "TimestampKey", getTimestampAsRow(123141, 431231), 3);

        assertEquals(8, featureRows.size());
        assertEquals("FloatKey", featureRows.get(0).getField(0));
        assertEquals("StringKey", featureRows.get(1).getField(0));
        assertEquals("DoubleKey", featureRows.get(2).getField(0));
        assertEquals("IntegerKey", featureRows.get(3).getField(0));
        assertEquals("LongKey", featureRows.get(4).getField(0));
        assertEquals("ByteKey", featureRows.get(5).getField(0));
        assertEquals("BoolKey", featureRows.get(6).getField(0));
        assertEquals("TimestampKey", featureRows.get(7).getField(0));
        assertEquals(1.0F, ((Row) featureRows.get(0).getField(1)).getField(5));
        assertEquals("stringValue", ((Row) featureRows.get(1).getField(1)).getField(1));
        assertEquals(1.00D, ((Row) featureRows.get(2).getField(1)).getField(4));
        assertEquals(1, ((Row) featureRows.get(3).getField(1)).getField(2));
        assertEquals(1L, ((Row) featureRows.get(4).getField(1)).getField(3));
        assertEquals(ByteString.copyFrom("value1".getBytes()), ((Row) featureRows.get(5).getField(1)).getField(0));
        assertEquals(true, ((Row) featureRows.get(6).getField(1)).getField(6));
        assertEquals(getTimestampAsRow(123141, 431231), ((Row) featureRows.get(7).getField(1)).getField(7));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForValuesItCannotHandle() {
        ArrayList<Row> featureRows = new ArrayList<>();
        FeatureUtils.populateFeatures(featureRows, "key1", "value".getBytes(), 3);
    }

    @Test
    public void shouldHandleAllValueTypesWithFeatureTypeUdf() {
        ArrayList<Row> featureRows = new ArrayList<>();
        FeatureUtils.populateFeaturesWithType(featureRows, "FloatKey", 1.0F, ValueEnum.FloatType, 3);
        FeatureUtils.populateFeaturesWithType(featureRows, "StringKey", "stringValue", ValueEnum.StringType, 3);
        FeatureUtils.populateFeaturesWithType(featureRows, "DoubleKey", 1.00D, ValueEnum.DoubleType, 3);
        FeatureUtils.populateFeaturesWithType(featureRows, "IntegerKey", 1, ValueEnum.IntegerType, 3);
        FeatureUtils.populateFeaturesWithType(featureRows, "LongKey", 1L, ValueEnum.LongType, 3);
        FeatureUtils.populateFeaturesWithType(featureRows, "ByteKey", ByteString.copyFrom("value1".getBytes()), ValueEnum.ByteType, 3);
        FeatureUtils.populateFeaturesWithType(featureRows, "BoolKey", true, ValueEnum.BooleanType, 3);
        FeatureUtils.populateFeaturesWithType(featureRows, "TimestampKey", getTimestampAsRow(123141, 431231), ValueEnum.TimestampType, 3);
        FeatureUtils.populateFeaturesWithType(featureRows, "BigDecimalKey", 123.0D, ValueEnum.DoubleType, 3);

        assertEquals(9, featureRows.size());
        assertEquals("FloatKey", featureRows.get(0).getField(0));
        assertEquals("StringKey", featureRows.get(1).getField(0));
        assertEquals("DoubleKey", featureRows.get(2).getField(0));
        assertEquals("IntegerKey", featureRows.get(3).getField(0));
        assertEquals("LongKey", featureRows.get(4).getField(0));
        assertEquals("ByteKey", featureRows.get(5).getField(0));
        assertEquals("BoolKey", featureRows.get(6).getField(0));
        assertEquals("TimestampKey", featureRows.get(7).getField(0));
        assertEquals("BigDecimalKey", featureRows.get(8).getField(0));
        assertEquals(1.0F, ((Row) featureRows.get(0).getField(1)).getField(5));
        assertEquals("stringValue", ((Row) featureRows.get(1).getField(1)).getField(1));
        assertEquals(1.00D, ((Row) featureRows.get(2).getField(1)).getField(4));
        assertEquals(1, ((Row) featureRows.get(3).getField(1)).getField(2));
        assertEquals(1L, ((Row) featureRows.get(4).getField(1)).getField(3));
        assertEquals(ByteString.copyFrom("value1".getBytes()), ((Row) featureRows.get(5).getField(1)).getField(0));
        assertEquals(true, ((Row) featureRows.get(6).getField(1)).getField(6));
        assertEquals(getTimestampAsRow(123141, 431231), ((Row) featureRows.get(7).getField(1)).getField(7));
        assertEquals(123.0D, ((Row) featureRows.get(8).getField(1)).getField(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForValuesItCannotHandleWithFeatureTypeUdf() {
        ArrayList<Row> featureRows = new ArrayList<>();
        FeatureUtils.populateFeaturesWithType(featureRows, "key1", "value".getBytes(), null, 3);
    }

    private Row getTimestampAsRow(int seconds, int nanos) {
        Row timestamp1 = new Row(2);
        timestamp1.setField(0, seconds);
        timestamp1.setField(1, nanos);
        return timestamp1;
    }
}
