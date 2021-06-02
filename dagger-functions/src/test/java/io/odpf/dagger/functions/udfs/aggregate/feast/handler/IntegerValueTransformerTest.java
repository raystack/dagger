package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class IntegerValueTransformerTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldReturnTrueForIntegerValues() {
        IntegerValueTransformer integerValueHandler = new IntegerValueTransformer();
        assertEquals(integerValueHandler.canTransform(2), true);
    }

    @Test
    public void shouldReturnValue() {
        IntegerValueTransformer integerValueHandler = new IntegerValueTransformer();
        assertEquals(1, integerValueHandler.transform(1).getField(2));
    }

    @Test
    public void shouldReturnDefaultValueForNull() {
        IntegerValueTransformer integerValueHandler = new IntegerValueTransformer();
        assertEquals(0, integerValueHandler.transform(null).getField(2));
    }

    @Test
    public void shouldTransformToIntegerValuesIfPossible() {
        IntegerValueTransformer intValueHandler = new IntegerValueTransformer();
        assertEquals("should transform long to int", 2, intValueHandler.transform(2L)
                .getField(2));
        assertEquals("should transform int sting to int", 2, intValueHandler.transform("2")
                .getField(2));
    }

    @Test
    public void shouldThrowErrorIfTranformationNotPossible() {
        thrown.expect(NumberFormatException.class);

        IntegerValueTransformer intValueHandler = new IntegerValueTransformer();
        intValueHandler.transform("random");
    }

}
