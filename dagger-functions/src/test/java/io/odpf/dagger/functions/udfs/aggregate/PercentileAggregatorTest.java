package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.PercentileAccumulator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

import static java.math.BigDecimal.valueOf;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PercentileAggregatorTest {

    @Test
    public void shouldReturn90Percentile() {
        PercentileAggregator pa = new PercentileAggregator();
        PercentileAccumulator acc = pa.createAccumulator();
        for (int i = 1; i <= 1000; i++) {
            acc.add(90, i);
        }
        Assert.assertEquals(900, pa.getValue(acc), 1);
    }

    @Test
    public void shouldMergeAccumulators() {
        PercentileAggregator percentileAggregator = new PercentileAggregator();
        PercentileAccumulator percentileAccumulator1 = new PercentileAccumulator();
        percentileAggregator.accumulate(percentileAccumulator1, valueOf(90D), valueOf(1D));
        percentileAggregator.accumulate(percentileAccumulator1, valueOf(90D), valueOf(2D));
        percentileAggregator.accumulate(percentileAccumulator1, valueOf(90D), valueOf(3D));

        PercentileAccumulator percentileAccumulator2 = new PercentileAccumulator();
        PercentileAccumulator percentileAccumulator3 = new PercentileAccumulator();
        percentileAccumulator2.add(90D, 4D);
        percentileAccumulator2.add(90D, 5D);
        percentileAccumulator3.add(90D, 6D);
        percentileAccumulator3.add(90D, 7D);


        Iterable<PercentileAccumulator> iterable = mock(Iterable.class);
        Iterator<PercentileAccumulator> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(percentileAccumulator2, percentileAccumulator3);

        percentileAggregator.merge(percentileAccumulator1, iterable);

        double result = percentileAggregator.getValue(percentileAccumulator1);
        assertEquals(7D, result, 0D);
    }

    @Test
    public void shouldNotChangeAccumulatorIfIterableIsEmptyOnMerge() {
        PercentileAggregator percentileAggregator = new PercentileAggregator();
        PercentileAccumulator percentileAccumulator = new PercentileAccumulator();
        percentileAggregator.accumulate(percentileAccumulator, valueOf(90D), valueOf(1D));
        percentileAggregator.accumulate(percentileAccumulator, valueOf(90D), valueOf(2D));
        percentileAggregator.accumulate(percentileAccumulator, valueOf(90D), valueOf(3D));

        Iterable<PercentileAccumulator> iterable = mock(Iterable.class);
        Iterator<PercentileAccumulator> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);

        percentileAggregator.merge(percentileAccumulator, iterable);

        double result = percentileAggregator.getValue(percentileAccumulator);
        assertEquals(3D, result, 0D);
    }
}
