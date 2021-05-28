package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BooleanValueTransformerTest {

    @Test
    public void shouldReturnTrueForBooleanValues() {
        BooleanValueTransformer booleanValueHandler = new BooleanValueTransformer();
        assertEquals(booleanValueHandler.canTransform(true), true);
    }

    @Test
    public void shouldReturnValue() {
        BooleanValueTransformer booleanValueHandler = new BooleanValueTransformer();
        Row rowBoolean = booleanValueHandler.transform(true);
        assertEquals(true, rowBoolean.getField(6));
    }

    @Test
    public void shouldReturnDefaultValueForNull() {
        BooleanValueTransformer booleanValueHandler = new BooleanValueTransformer();
        Row rowBoolean = booleanValueHandler.transform(null);
        assertEquals(false, rowBoolean.getField(6));
    }
}
