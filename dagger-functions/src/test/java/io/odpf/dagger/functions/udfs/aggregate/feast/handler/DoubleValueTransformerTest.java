package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.apache.flink.types.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class DoubleValueTransformerTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldReturnTrueForDoubleValues() {
        DoubleValueTransformer doubleValueHandler = new DoubleValueTransformer();
        assertEquals(doubleValueHandler.canTransform(2.00D), true);
    }

    @Test
    public void shouldReturnValue() {
        DoubleValueTransformer doubleValueHandler = new DoubleValueTransformer();
        Row doubleRow = doubleValueHandler.transform(2.00D);
        assertEquals(2.00D, doubleRow.getField(4));
    }

    @Test
    public void shouldReturnDefaultValueForNull() {
        DoubleValueTransformer doubleValueHandler = new DoubleValueTransformer();
        Row doubleRow = doubleValueHandler.transform(null);
        assertEquals(0.0D, doubleRow.getField(4));
    }

    @Test
    public void shouldTransformToDoubleValuesIfPossible() {
        DoubleValueTransformer doubleValueHandler = new DoubleValueTransformer();
        assertEquals("should transform int to double", 2.0D, doubleValueHandler.transform(2)
                .getField(4));
        assertEquals("should transform double sting to double", 2.0D, doubleValueHandler.transform("2.0")
                .getField(4));
        assertEquals("should transform float to double", 2.0D, doubleValueHandler.transform(2.0f)
                .getField(4));
        assertEquals("should transform long to double", 2.0D, doubleValueHandler.transform(2L)
                .getField(4));
    }

    @Test
    public void shouldThrowErrorIfTranformationNotPossible() {
        thrown.expect(NumberFormatException.class);

        DoubleValueTransformer doubleValueHandler = new DoubleValueTransformer();
        doubleValueHandler.transform("random");
    }
}
