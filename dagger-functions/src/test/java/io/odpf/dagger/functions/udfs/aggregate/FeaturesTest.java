package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.FeatureAccumulator;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Iterator;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
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

        Iterable<FeatureAccumulator> iterable = mock(Iterable.class);
        Iterator<FeatureAccumulator> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(featureAccumulator2, featureAccumulator3);

        features.merge(featureAccumulator1, iterable);

        Row[] result = features.getValue(featureAccumulator1);
        assertEquals(7, result.length);
        assertEquals("key1", result[0].getField(0));
        assertEquals("key2", result[1].getField(0));
        assertEquals("key3", result[2].getField(0));
        assertEquals("key4", result[3].getField(0));
        assertEquals("key5", result[4].getField(0));
        assertEquals("key6", result[5].getField(0));
        assertEquals("key7", result[6].getField(0));
        assertEquals("value1", ((Row) result[0].getField(1)).getField(1));
        assertEquals("value2", ((Row) result[1].getField(1)).getField(1));
        assertEquals("value3", ((Row) result[2].getField(1)).getField(1));
        assertEquals("value4", ((Row) result[3].getField(1)).getField(1));
        assertEquals("value5", ((Row) result[4].getField(1)).getField(1));
        assertEquals("value6", ((Row) result[5].getField(1)).getField(1));
        assertEquals("value7", ((Row) result[6].getField(1)).getField(1));
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

        Iterable<FeatureAccumulator> iterable = mock(Iterable.class);
        Iterator<FeatureAccumulator> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(featureAccumulator2, featureAccumulator3);

        features.merge(featureAccumulator1, iterable);

        Row[] result = features.getValue(featureAccumulator1);
        assertEquals(3, result.length);
        assertEquals("key1", result[0].getField(0));
        assertEquals("key2", result[1].getField(0));
        assertEquals("key3", result[2].getField(0));
        assertEquals("value1", ((Row) result[0].getField(1)).getField(1));
        assertEquals("value2", ((Row) result[1].getField(1)).getField(1));
        assertEquals("value3", ((Row) result[2].getField(1)).getField(1));
    }
}
