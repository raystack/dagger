package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import org.junit.Assert;
import org.junit.Test;

public class PercentileAccumulatorTest {

    // Just calling apache commons so not many tests are needed.
    @Test
    public void shouldReturn90thPercentileValue() {
        PercentileAccumulator accumulator = new PercentileAccumulator();
        accumulator.add(90, 10);
        accumulator.add(90, 30);
        accumulator.add(90, 40);
        accumulator.add(90, 20);
        accumulator.add(90, 70);
        accumulator.add(90, 90);
        accumulator.add(90, 100);
        accumulator.add(90, 80);
        accumulator.add(90, 50);
        accumulator.add(90, 60);
        Assert.assertEquals(99.0, accumulator.getPercentileValue(), 0.001);
    }
}
