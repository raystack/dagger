package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.FeatureAccumulator;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class FeaturesTest {

    @Mock
    private FeatureAccumulator featureAccumulator;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateFeatureAccumulator() {
        Features features = new Features();
        FeatureAccumulator accumulator = features.createAccumulator();
        assertNotNull(accumulator);
        assertEquals(accumulator.getClass(), FeatureAccumulator.class);
    }

    @Test
    public void shouldAddEntriesToAccumulator() {
        Features features = new Features();
        features.accumulate(featureAccumulator, "key1", "value1");
        features.accumulate(featureAccumulator, "key2", "value2");
        features.accumulate(featureAccumulator, "key3", "value3");

        verify(featureAccumulator, times(3)).add(anyString(), anyString());
    }

    @Test
    public void shouldAddArbitraryNumberOfKVsToAccumulator() {
        Features features = new Features();
        features.accumulate(featureAccumulator, "key1", "value1", "key2", "value2");

        verify(featureAccumulator, times(2)).add(anyString(), anyString());
    }

    @Test
    public void shouldGetFeaturesFromAccumulator() {
        Features features = new Features();
        features.getValue(featureAccumulator);

        verify(featureAccumulator, times(1)).getFeaturesAsRows();
    }

    @Test
    public void shouldMergeAccumulators() {
        Features features = new Features();
        FeatureAccumulator featureAccumulator1 = new FeatureAccumulator();
        features.accumulate(featureAccumulator1, "key1", "value1");
        features.accumulate(featureAccumulator1, "key2", "value2");
        features.accumulate(featureAccumulator1, "key3", "value3");

        FeatureAccumulator featureAccumulator2 = new FeatureAccumulator();
        FeatureAccumulator featureAccumulator3 = new FeatureAccumulator();
        featureAccumulator2.add("key4", "value4");
        featureAccumulator2.add("key5", "value5");
        featureAccumulator3.add("key6", "value6");
        featureAccumulator3.add("key7", "value7");

        ArrayList<FeatureAccumulator> iterable = new ArrayList<>();
        iterable.add(featureAccumulator2);
        iterable.add(featureAccumulator3);

        features.merge(featureAccumulator1, iterable);

        Row row1 = new Row(3);
        Row row2 = new Row(3);
        Row row3 = new Row(3);
        Row row4 = new Row(3);
        Row row5 = new Row(3);
        Row row6 = new Row(3);
        Row row7 = new Row(3);

        Row[] expectedRows = new Row[]{row1, row2, row3, row4, row5, row6, row7};
        expectedRows[0].setField(0, "key1");
        expectedRows[0].setField(2, "key1");
        expectedRows[1].setField(0, "key2");
        expectedRows[1].setField(2, "key2");
        expectedRows[2].setField(0, "key3");
        expectedRows[2].setField(2, "key3");
        expectedRows[3].setField(0, "key4");
        expectedRows[3].setField(2, "key4");
        expectedRows[4].setField(0, "key5");
        expectedRows[4].setField(2, "key5");
        expectedRows[5].setField(0, "key6");
        expectedRows[5].setField(2, "key6");
        expectedRows[6].setField(0, "key7");
        expectedRows[6].setField(2, "key7");
        Row nestedRow1 = new Row(8);
        Row nestedRow2 = new Row(8);
        Row nestedRow3 = new Row(8);
        Row nestedRow4 = new Row(8);
        Row nestedRow5 = new Row(8);
        Row nestedRow6 = new Row(8);
        Row nestedRow7 = new Row(8);
        nestedRow1.setField(1, "value1");
        nestedRow2.setField(1, "value2");
        nestedRow3.setField(1, "value3");
        nestedRow4.setField(1, "value4");
        nestedRow5.setField(1, "value5");
        nestedRow6.setField(1, "value6");
        nestedRow7.setField(1, "value7");
        expectedRows[0].setField(1, nestedRow1);
        expectedRows[1].setField(1, nestedRow2);
        expectedRows[2].setField(1, nestedRow3);
        expectedRows[3].setField(1, nestedRow4);
        expectedRows[4].setField(1, nestedRow5);
        expectedRows[5].setField(1, nestedRow6);
        expectedRows[6].setField(1, nestedRow7);

        Row[] result = features.getValue(featureAccumulator1);
        assertArrayEquals(expectedRows, result);
    }

    @Test
    public void shouldNotChangeAccumulatorIfIterableIsEmptyOnMerge() {
        Features features = new Features();
        FeatureAccumulator featureAccumulator1 = new FeatureAccumulator();
        features.accumulate(featureAccumulator1, "key1", "value1");
        features.accumulate(featureAccumulator1, "key2", "value2");
        features.accumulate(featureAccumulator1, "key3", "value3");

        FeatureAccumulator featureAccumulator2 = new FeatureAccumulator();
        FeatureAccumulator featureAccumulator3 = new FeatureAccumulator();

        ArrayList<FeatureAccumulator> iterable = new ArrayList<>();
        iterable.add(featureAccumulator2);
        iterable.add(featureAccumulator3);

        features.merge(featureAccumulator1, iterable);

        Row row1 = new Row(3);
        Row row2 = new Row(3);
        Row row3 = new Row(3);

        Row[] expectedRows = new Row[]{row1, row2, row3};
        expectedRows[0].setField(0, "key1");
        expectedRows[0].setField(2, "key1");
        expectedRows[1].setField(0, "key2");
        expectedRows[1].setField(2, "key2");
        expectedRows[2].setField(0, "key3");
        expectedRows[2].setField(2, "key3");
        Row nestedRow1 = new Row(8);
        Row nestedRow2 = new Row(8);
        Row nestedRow3 = new Row(8);
        nestedRow1.setField(1, "value1");
        nestedRow2.setField(1, "value2");
        nestedRow3.setField(1, "value3");
        expectedRows[0].setField(1, nestedRow1);
        expectedRows[1].setField(1, nestedRow2);
        expectedRows[2].setField(1, nestedRow3);

        Row[] result = features.getValue(featureAccumulator1);
        assertArrayEquals(expectedRows, result);
    }
}
