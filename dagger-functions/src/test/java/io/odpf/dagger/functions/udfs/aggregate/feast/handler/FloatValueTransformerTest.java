package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class FloatValueTransformerTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldReturnTrueForFloatValues() {
        FloatValueTransformer floatValueHandler = new FloatValueTransformer();
        assertEquals(floatValueHandler.canTransform(2.0F), true);
    }

    @Test
    public void shouldReturnValue() {
        FloatValueTransformer floatValueHandler = new FloatValueTransformer();
        assertEquals(2.0F, floatValueHandler.transform(2.0F).getField(5));
    }

    @Test
    public void shouldReturnDefaultValueForNull() {
        FloatValueTransformer floatValueHandler = new FloatValueTransformer();
        assertEquals(0.0F, floatValueHandler.transform(null).getField(5));
    }

    @Test
    public void shouldTransformToFloatValuesIfPossible() {
        FloatValueTransformer floatValueHandler = new FloatValueTransformer();
        assertEquals("should transform int to float", 2.0F, floatValueHandler.transform(2)
                .getField(5));
        assertEquals("should transform float sting to float", 2.0F, floatValueHandler.transform("2.0")
                .getField(5));
        assertEquals("should transform double to float", 2.0F, floatValueHandler.transform(2d)
                .getField(5));
    }

    @Test
    public void shouldThrowErrorIfTranformationNotPossible() {
        thrown.expect(NumberFormatException.class);

        FloatValueTransformer floatValueHandler = new FloatValueTransformer();
        floatValueHandler.transform("random");
    }
}
