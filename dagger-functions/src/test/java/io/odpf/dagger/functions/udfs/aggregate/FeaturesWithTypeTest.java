package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.exceptions.InvalidNumberOfArgumentsException;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.FeatureWithTypeAccumulator;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.gradle.internal.impldep.org.testng.AssertJUnit.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class FeaturesWithTypeTest {
    @Mock
    private FeatureWithTypeAccumulator featureAccumulator;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateFeatureAccumulator() {
        FeaturesWithType features = new FeaturesWithType();
        FeatureWithTypeAccumulator accumulator = features.createAccumulator();
        assertNotNull(accumulator);
        assertEquals(accumulator.getClass(), FeatureWithTypeAccumulator.class);
    }

    @Test
    public void shouldAddEntriesToAccumulator() {
        FeaturesWithType features = new FeaturesWithType();
        features.accumulate(featureAccumulator, "key1", "value1", "StringType");
        features.accumulate(featureAccumulator, "key2", "value2", "StringType");
        features.accumulate(featureAccumulator, "key3", "value3", "StringType");

        verify(featureAccumulator, times(3)).add(anyString(), anyString(), any(ValueEnum.class));
    }

    @Test
    public void shouldRemoveEntriesToAccumulator() {
        FeaturesWithType features = new FeaturesWithType();
        features.retract(featureAccumulator, "key1", "value1", "StringType");
        features.retract(featureAccumulator, "key2", "value2", "StringType");
        features.retract(featureAccumulator, "key3", "value3", "StringType");

        verify(featureAccumulator, times(3)).remove(anyString(), anyString(), any(ValueEnum.class));
    }

    @Test
    public void shouldAddSixArgumentsAsTwoBatchToAccumulator() {
        FeaturesWithType features = new FeaturesWithType();
        features.accumulate(featureAccumulator, "key1", "value1", "StringType", "key2", "value2", "StringType");

        verify(featureAccumulator, times(2)).add(anyString(), anyString(), any(ValueEnum.class));
    }

    @Test
    public void shouldRemoveSixArgumentsAsTwoBatchFromAccumulator() {
        FeaturesWithType features = new FeaturesWithType();
        features.retract(featureAccumulator, "key1", "value1", "StringType", "key2", "value2", "StringType");

        verify(featureAccumulator, times(2)).remove(anyString(), anyString(), any(ValueEnum.class));
    }

    @Test
    public void shouldGetFeaturesFromAccumulator() {
        FeaturesWithType features = new FeaturesWithType();
        features.getValue(featureAccumulator);

        verify(featureAccumulator, times(1)).getFeaturesAsRows();
    }

    @Test(expected = InvalidNumberOfArgumentsException.class)
    public void shouldThrowExceptionWhenNumberOfArgumentIsOneForAccumulate() {
        FeaturesWithType features = new FeaturesWithType();

        features.accumulate(featureAccumulator, "one");
    }

    @Test(expected = InvalidNumberOfArgumentsException.class)
    public void shouldThrowExceptionWhenNumberOfArgumentIsTwoForAccumulate() {
        FeaturesWithType features = new FeaturesWithType();

        features.accumulate(featureAccumulator, "one", "two");
    }

    @Test(expected = InvalidNumberOfArgumentsException.class)
    public void shouldThrowExceptionWhenNumberOfArgumentIsFourForAccumulate() {
        FeaturesWithType features = new FeaturesWithType();

        features.accumulate(featureAccumulator, "one", "two", "three", "four");
    }

    @Test(expected = InvalidNumberOfArgumentsException.class)
    public void shouldThrowExceptionWhenNumberOfArgumentIsOneForRetract() {
        FeaturesWithType features = new FeaturesWithType();

        features.retract(featureAccumulator, "one");
    }

    @Test(expected = InvalidNumberOfArgumentsException.class)
    public void shouldThrowExceptionWhenNumberOfArgumentIsTwoForRetract() {
        FeaturesWithType features = new FeaturesWithType();

        features.retract(featureAccumulator, "one", "two");
    }

    @Test(expected = InvalidNumberOfArgumentsException.class)
    public void shouldThrowExceptionWhenNumberOfArgumentIsFourForRetract() {
        FeaturesWithType features = new FeaturesWithType();

        features.retract(featureAccumulator, "one", "two", "three", "four");
    }

    @Test
    public void shouldMergeAccumulators() {
        FeaturesWithType featuresWithType = new FeaturesWithType();
        FeatureWithTypeAccumulator featureWithTypeAccumulator1 = new FeatureWithTypeAccumulator();
        featuresWithType.accumulate(featureWithTypeAccumulator1, "key1", "value1", "StringType");
        featuresWithType.accumulate(featureWithTypeAccumulator1, "key2", "value2", "StringType");
        featuresWithType.accumulate(featureWithTypeAccumulator1, "key3", "value3", "StringType");

        FeatureWithTypeAccumulator featureWithTypeAccumulator2 = new FeatureWithTypeAccumulator();
        FeatureWithTypeAccumulator featureWithTypeAccumulator3 = new FeatureWithTypeAccumulator();
        featureWithTypeAccumulator2.add("key4", "value4", ValueEnum.StringType);
        featureWithTypeAccumulator2.add("key5", "value5", ValueEnum.StringType);
        featureWithTypeAccumulator3.add("key6", "value6", ValueEnum.StringType);
        featureWithTypeAccumulator3.add("key7", "value7", ValueEnum.StringType);

        ArrayList<FeatureWithTypeAccumulator> iterable = new ArrayList<>();
        iterable.add(featureWithTypeAccumulator2);
        iterable.add(featureWithTypeAccumulator3);

        featuresWithType.merge(featureWithTypeAccumulator1, iterable);

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

        Row[] result = featuresWithType.getValue(featureWithTypeAccumulator1);
        List<Row> rowList = Arrays.asList(result);
        assertEquals(7, result.length);
        assertTrue(Arrays.stream(expectedRows).allMatch(rowList::contains));
    }

    @Test
    public void shouldNotChangeAccumulatorIfIterableIsEmptyOnMerge() {
        FeaturesWithType featuresWithType = new FeaturesWithType();
        FeatureWithTypeAccumulator featureWithTypeAccumulator1 = new FeatureWithTypeAccumulator();
        featuresWithType.accumulate(featureWithTypeAccumulator1, "key1", "value1", "StringType");
        featuresWithType.accumulate(featureWithTypeAccumulator1, "key2", "value2", "StringType");
        featuresWithType.accumulate(featureWithTypeAccumulator1, "key3", "value3", "StringType");

        FeatureWithTypeAccumulator featureWithTypeAccumulator2 = new FeatureWithTypeAccumulator();
        FeatureWithTypeAccumulator featureWithTypeAccumulator3 = new FeatureWithTypeAccumulator();

        ArrayList<FeatureWithTypeAccumulator> iterable = new ArrayList<>();
        iterable.add(featureWithTypeAccumulator2);
        iterable.add(featureWithTypeAccumulator3);

        featuresWithType.merge(featureWithTypeAccumulator1, iterable);

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

        Row[] result = featuresWithType.getValue(featureWithTypeAccumulator1);
        List<Row> rowList = Arrays.asList(result);
        assertEquals(3, result.length);
        assertTrue(Arrays.stream(expectedRows).allMatch(rowList::contains));
    }

}
