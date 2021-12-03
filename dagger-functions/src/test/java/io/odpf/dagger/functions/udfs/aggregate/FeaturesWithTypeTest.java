package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.exceptions.InvalidNumberOfArgumentsException;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.FeatureWithTypeAccumulator;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import junit.framework.TestCase;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Iterator;

import static org.gradle.internal.impldep.org.testng.AssertJUnit.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
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

        Iterable<FeatureWithTypeAccumulator> iterable = mock(Iterable.class);
        Iterator<FeatureWithTypeAccumulator> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(featureWithTypeAccumulator2, featureWithTypeAccumulator3);

        featuresWithType.merge(featureWithTypeAccumulator1, iterable);

        Row[] result = featuresWithType.getValue(featureWithTypeAccumulator1);
        TestCase.assertEquals(7, result.length);
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

        Iterable<FeatureWithTypeAccumulator> iterable = mock(Iterable.class);
        Iterator<FeatureWithTypeAccumulator> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(featureWithTypeAccumulator2, featureWithTypeAccumulator3);

        featuresWithType.merge(featureWithTypeAccumulator1, iterable);

        Row[] result = featuresWithType.getValue(featureWithTypeAccumulator1);
        TestCase.assertEquals(3, result.length);
    }

}
