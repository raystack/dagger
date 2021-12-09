package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.PercentileAccumulator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static java.math.BigDecimal.valueOf;
import static org.junit.Assert.assertEquals;

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


        ArrayList<PercentileAccumulator> iterable = new ArrayList<>();
        iterable.add(percentileAccumulator2);
        iterable.add(percentileAccumulator3);

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

        ArrayList<PercentileAccumulator> iterable = new ArrayList<>();

        percentileAggregator.merge(percentileAccumulator, iterable);

        double result = percentileAggregator.getValue(percentileAccumulator);
        assertEquals(3D, result, 0D);
    }
}
