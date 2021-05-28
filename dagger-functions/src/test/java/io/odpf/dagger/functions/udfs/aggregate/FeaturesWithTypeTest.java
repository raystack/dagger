package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.exceptions.InvalidNumberOfArgumentsException;
import io.odpf.dagger.functions.udfs.aggregate.accumulator.FeatureWithTypeAccumulator;
import io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.gradle.internal.impldep.org.testng.AssertJUnit.assertNotNull;
import static org.junit.Assert.assertEquals;
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

        verify(featureAccumulator, times(1)).getFeatures();
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

}
