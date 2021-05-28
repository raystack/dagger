package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimestampValueTransformerTest {

    @Test
    public void shouldReturnTrueForTimestampValues() {
        TimestampValueTransformer timestampValueHandler = new TimestampValueTransformer();
        assertEquals(timestampValueHandler.canTransform(getTimestampAsRow(123141, 431231)), true);
    }

    @Test
    public void shouldReturnFalseIfRowArityIsNotTwo() {
        Row row = new Row(1);
        TimestampValueTransformer timestampValueHandler = new TimestampValueTransformer();
        assertEquals(false, timestampValueHandler.canTransform(row));
    }

    @Test
    public void shouldReturnFalseIfRowIsNotPassed() {
        TimestampValueTransformer timestampValueHandler = new TimestampValueTransformer();
        assertEquals(false, timestampValueHandler.canTransform(null));
    }

    @Test
    public void shouldReturnValueInRow() {
        Row timestamp = getTimestampAsRow(123141, 431231);
        TimestampValueTransformer timestampValueHandler = new TimestampValueTransformer();
        Row timestampRow = timestampValueHandler.transform(timestamp);
        assertEquals(timestamp, timestampRow.getField(7));
    }

    private Row getTimestampAsRow(int seconds, int nanos) {
        Row timestamp1 = new Row(2);
        timestamp1.setField(0, seconds);
        timestamp1.setField(1, nanos);
        return timestamp1;
    }
}
