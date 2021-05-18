package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class LongValueTransformerTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldReturnTrueForLongValues() {
        LongValueTransformer longValueHandler = new LongValueTransformer();
        assertEquals(longValueHandler.canTransform(1L), true);
    }

    @Test
    public void shouldReturnValue() {
        LongValueTransformer longValueHandler = new LongValueTransformer();
        assertEquals(1L, longValueHandler.transform(1L).getField(3));
    }

    @Test
    public void shouldReturnDefaultValueForNull() {
        LongValueTransformer longValueHandler = new LongValueTransformer();
        assertEquals(0L, longValueHandler.transform(null).getField(3));
    }

    @Test
    public void shouldTransformToLongValuesIfPossible() {
        LongValueTransformer longValueHandler = new LongValueTransformer();
        assertEquals("should transform int to long", 2L, longValueHandler.transform(2)
                .getField(3));
        assertEquals("should transform long sting to long", 2L, longValueHandler.transform("2")
                .getField(3));
    }

    @Test
    public void shouldThrowErrorIfTranformationNotPossible() {
        thrown.expect(NumberFormatException.class);

        LongValueTransformer longValueHandler = new LongValueTransformer();
        longValueHandler.transform("random");
    }
}
