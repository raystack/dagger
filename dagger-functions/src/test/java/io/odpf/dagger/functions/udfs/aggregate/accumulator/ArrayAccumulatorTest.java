package io.odpf.dagger.functions.udfs.aggregate.accumulator;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ArrayAccumulatorTest {
    @Test
    public void shouldGiveAllValuesFromAccumulator() {
        ArrayAccumulator arrayAccumulator = new ArrayAccumulator();
        String value1 = "first";
        String value2 = "second";

        arrayAccumulator.add(value1);
        arrayAccumulator.add(value2);

        List<Object> values = arrayAccumulator.emit();
        assertEquals(2, values.size());
        assertEquals("first", (String) values.get(0));
        assertEquals("second", (String) values.get(1));
    }
}
