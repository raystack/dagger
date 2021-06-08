package io.odpf.dagger.functions.udfs.scalar;

import com.google.protobuf.ByteString;
import io.odpf.dagger.functions.exceptions.InvalidNumberOfArgumentsException;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

public class SingleFeatureWithTypeTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldAddOneEntry() {
        SingleFeatureWithType singleFeatureWithTypeUdf = new SingleFeatureWithType();

        Row[] feature = singleFeatureWithTypeUdf.eval("key1", "value1", "StringType");

        assertEquals("key1", feature[0].getField(0));
        assertEquals("value1", ((Row) feature[0].getField(1)).getField(1));
    }

    @Test
    public void shouldHandleMultipleDataTypeOfAllKinds() {
        SingleFeatureWithType singleFeatureWithTypeUdf = new SingleFeatureWithType();
        HashMap<String, Tuple2<Object, ValueEnum>> data = new HashMap<>();
        data.put("FloatKey", new Tuple2<>(1.0F, FloatType));
        data.put("StringKey", new Tuple2<>("stringValue", StringType));
        data.put("DoubleKey", new Tuple2<>(1.00D, DoubleType));
        data.put("IntegerKey", new Tuple2<>(1, IntegerType));
        data.put("LongKey", new Tuple2<>(1L, LongType));
        data.put("ByteKey", new Tuple2<>(ByteString.copyFrom("value1".getBytes()), ByteType));
        data.put("BoolKey", new Tuple2<>(true, BooleanType));
        data.put("TimestampKey", new Tuple2<>(getTimestampAsRow(123141, 431231), TimestampType));

        ArrayList<Object> arrayData = new ArrayList<>();
        for (Map.Entry<String, Tuple2<Object, ValueEnum>> entry : data.entrySet()) {
            arrayData.add(entry.getKey());
            arrayData.add(entry.getValue().f0);
            arrayData.add(entry.getValue().f1);
        }
        Row[] features = singleFeatureWithTypeUdf.eval(arrayData.toArray());

        validateFeatures(data, features);
    }

    @Test
    public void shouldThrowErrorWhenNumberOfArgumentsIsOne() {
        expectedException.expect(InvalidNumberOfArgumentsException.class);

        SingleFeatureWithType singleFeatureWithType = new SingleFeatureWithType();

        singleFeatureWithType.eval("one");
    }

    @Test
    public void shouldThrowErrorWhenNumberOfArgumentsIsTwo() {
        expectedException.expect(InvalidNumberOfArgumentsException.class);

        SingleFeatureWithType singleFeatureWithType = new SingleFeatureWithType();

        singleFeatureWithType.eval("one", "two");
    }

    @Test
    public void shouldThrowErrorWhenNumberOfArgumentsIsFour() {
        expectedException.expect(InvalidNumberOfArgumentsException.class);

        SingleFeatureWithType singleFeatureWithType = new SingleFeatureWithType();

        singleFeatureWithType.eval("one", "two", "three", "four");
    }

    @Test
    public void shouldThrowExceptionForValuesItCannotHandleWithFeatureTypeUdf() {
        expectedException.expect(IllegalArgumentException.class);

        SingleFeatureWithType singleFeatureWithType = new SingleFeatureWithType();

        singleFeatureWithType.eval("one", "two", null);
    }


    private void validateFeatures(HashMap<String, Tuple2<Object, ValueEnum>> data, Row[] features) {
        assertEquals(data.size(), features.length);
        HashMap<String, Boolean> uniqueKeysPresentInFeatures = new HashMap<>();
        for (Row feature : features) {
            String key = (String) feature.getField(0);
            uniqueKeysPresentInFeatures.put(key, true);
            assertTrue(String.format("%s was not added in the data to create features", key), data.containsKey(key));
            assertEquals("%s data didn't match", data.get(key).f0, ((Row) feature.getField(1)).getField(data.get(key).f1.getValue()));
        }
        assertEquals(data.size(), uniqueKeysPresentInFeatures.size());
    }


    private Row getTimestampAsRow(int seconds, int nanos) {
        Row timestamp1 = new Row(2);
        timestamp1.setField(0, seconds);
        timestamp1.setField(1, nanos);
        return timestamp1;
    }
}
