package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.PercentileAccumulator;
import org.junit.Assert;
import org.junit.Test;

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
}
