package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.FeatureAccumulator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
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

        verify(featureAccumulator, times(1)).getFeatures();
    }
}
