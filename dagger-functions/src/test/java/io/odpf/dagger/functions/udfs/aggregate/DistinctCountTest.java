package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.distinctcount.DistinctCountAccumulator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DistinctCountTest {

    @Test
    public void shouldNotAddItemIfItAlreadyExistsInDistinctItems() {
        DistinctCountAccumulator distinctCountAccumulator = new DistinctCountAccumulator();
        DistinctCount distinctCount = new DistinctCount();
        distinctCount.accumulate(distinctCountAccumulator, "1234");
        distinctCount.accumulate(distinctCountAccumulator, "1234");
        distinctCount.accumulate(distinctCountAccumulator, "1233");
        assertEquals(new Integer(2), distinctCount.getValue(distinctCountAccumulator));
    }

    @Test
    public void shouldNotAddNull() {
        DistinctCountAccumulator distinctCountAccumulator = new DistinctCountAccumulator();
        DistinctCount distinctCount = new DistinctCount();
        distinctCount.accumulate(distinctCountAccumulator, null);
        assertEquals(new Integer(0), distinctCount.getValue(distinctCountAccumulator));
    }

    @Test
    public void shouldNotHoldState() {
        DistinctCount distinctCount = new DistinctCount();
        DistinctCountAccumulator acc1 = distinctCount.createAccumulator();
        DistinctCountAccumulator acc2 = distinctCount.createAccumulator();

        distinctCount.accumulate(acc1, "111");
        distinctCount.accumulate(acc1, "222");
        distinctCount.accumulate(acc1, "222");
        distinctCount.accumulate(acc1, "333");

        distinctCount.accumulate(acc2, "444");
        distinctCount.accumulate(acc2, "555");

        assertEquals(new Integer(3), distinctCount.getValue(acc1));
        assertEquals(new Integer(2), distinctCount.getValue(acc2));
    }
}
