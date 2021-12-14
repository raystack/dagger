package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.StringType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class FeatureAccumulatorTest {
    @Test
    public void shouldBeSerializable() throws IOException, ClassNotFoundException {
        FeatureAccumulator featureAccumulator = new FeatureAccumulator();
        featureAccumulator.add("key1", "value1");
        featureAccumulator.add("key2", "value2");
        featureAccumulator.add("key3", "value3");

        ByteArrayOutputStream serializedAccumulatorStream = new ByteArrayOutputStream();
        new ObjectOutputStream(serializedAccumulatorStream).writeObject(featureAccumulator);

        ObjectInputStream deserializedAccStream = new ObjectInputStream(new ByteArrayInputStream(serializedAccumulatorStream.toByteArray()));

        FeatureAccumulator deserializedAccumulator = (FeatureAccumulator) deserializedAccStream.readObject();

        assertArrayEquals(featureAccumulator.getFeaturesAsRows(), deserializedAccumulator.getFeaturesAsRows());
    }

    @Test
    public void shouldHandleAllValueTypes() {
        FeatureAccumulator featureAccumulator = new FeatureAccumulator();
        featureAccumulator.add("FloatKey", 1.0F);
        featureAccumulator.add("StringKey", "stringValue");
        featureAccumulator.add("DoubleKey", 1.00D);
        featureAccumulator.add("IntegerKey", 1);
        featureAccumulator.add("LongKey", 1L);
        featureAccumulator.add("ByteKey", ByteString.copyFrom("value1".getBytes()));
        featureAccumulator.add("BoolKey", true);
        featureAccumulator.add("TimestampKey", getTimestampAsRow(123141, 431231));

        Row[] features = featureAccumulator.getFeaturesAsRows();

        assertEquals(8, features.length);
        assertEquals("FloatKey", features[0].getField(0));
        assertEquals("StringKey", features[1].getField(0));
        assertEquals("DoubleKey", features[2].getField(0));
        assertEquals("IntegerKey", features[3].getField(0));
        assertEquals("LongKey", features[4].getField(0));
        assertEquals("ByteKey", features[5].getField(0));
        assertEquals("BoolKey", features[6].getField(0));
        assertEquals("TimestampKey", features[7].getField(0));
        assertEquals(1.0F, ((Row) features[0].getField(1)).getField(5));
        assertEquals("stringValue", ((Row) features[1].getField(1)).getField(1));
        assertEquals(1.00D, ((Row) features[2].getField(1)).getField(4));
        assertEquals(1, ((Row) features[3].getField(1)).getField(2));
        assertEquals(1L, ((Row) features[4].getField(1)).getField(3));
        assertEquals(ByteString.copyFrom("value1".getBytes()), ((Row) features[5].getField(1)).getField(0));
        assertEquals(true, ((Row) features[6].getField(1)).getField(6));
        assertEquals(getTimestampAsRow(123141, 431231), ((Row) features[7].getField(1)).getField(7));
    }

    @Test
    public void shouldPopulateIdAndNameWithSameValuesInFeatureRow() {
        FeatureAccumulator featureAccumulator = new FeatureAccumulator();
        featureAccumulator.add("FloatKey", 1.0F);

        Row[] features = featureAccumulator.getFeaturesAsRows();

        assertEquals(1, features.length);
        assertEquals("FloatKey", features[0].getField(0));
        assertEquals("FloatKey", features[0].getField(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForValuesItCannotHandle() {
        FeatureAccumulator featureAccumulator = new FeatureAccumulator();
        featureAccumulator.add("key1", "value".getBytes());

        featureAccumulator.getFeaturesAsRows();
    }

    @Test
    public void shouldBeSerializableWithFeatureTypeUdf() throws IOException, ClassNotFoundException {
        FeatureWithTypeAccumulator featureAccumulator = new FeatureWithTypeAccumulator();
        featureAccumulator.add("key1", "value1", StringType);
        featureAccumulator.add("key2", "value2", StringType);
        featureAccumulator.add("key3", "value3", StringType);
        ByteArrayOutputStream serializedAccumulatorStream = new ByteArrayOutputStream();
        new ObjectOutputStream(serializedAccumulatorStream).writeObject(featureAccumulator);
        ObjectInputStream deserializedAccStream = new ObjectInputStream(new ByteArrayInputStream(serializedAccumulatorStream.toByteArray()));

        FeatureWithTypeAccumulator deserializedAccumulator = (FeatureWithTypeAccumulator) deserializedAccStream.readObject();

        assertArrayEquals(featureAccumulator.getFeaturesAsRows(), deserializedAccumulator.getFeaturesAsRows());
    }

    private Row getTimestampAsRow(int seconds, int nanos) {
        Row timestamp1 = new Row(2);
        timestamp1.setField(0, seconds);
        timestamp1.setField(1, nanos);
        return timestamp1;
    }
}
