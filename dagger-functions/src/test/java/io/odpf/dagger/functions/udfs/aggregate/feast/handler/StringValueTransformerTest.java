package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringValueTransformerTest {

    @Test
    public void shouldReturnTrueForStringValues() {
        StringValueTransformer stringValueHandler = new StringValueTransformer();
        assertEquals(stringValueHandler.canTransform("value"), true);
    }

    @Test
    public void shouldReturnValue() {
        StringValueTransformer stringValueHandler = new StringValueTransformer();
        assertEquals("value", stringValueHandler.transform("value").getField(1));
    }

    @Test
    public void shouldTransformAnyObjectToString() {
        StringValueTransformer stringValueHandler = new StringValueTransformer();
        assertEquals("1", stringValueHandler.transform(1).getField(1));
    }
}
