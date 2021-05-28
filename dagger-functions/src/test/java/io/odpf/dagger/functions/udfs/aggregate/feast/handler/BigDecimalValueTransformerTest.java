package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BigDecimalValueTransformerTest {

    @Test
    public void shouldReturnTrueForBigDecimalValues() {
        BigDecimalValueTransformer bigDecimalValueTransformer = new BigDecimalValueTransformer();
        assertEquals(bigDecimalValueTransformer.canTransform(new BigDecimal(123)), true);
    }

    @Test
    public void shouldReturnDoubleValue() {
        BigDecimalValueTransformer bigDecimalValueTransformer = new BigDecimalValueTransformer();
        Row rowBigDecimal = bigDecimalValueTransformer.transform(new BigDecimal(123));
        assertEquals(123.0D, rowBigDecimal.getField(4));
        assertTrue(rowBigDecimal.getField(4) instanceof Double);
    }

    @Test
    public void shouldReturnDefaultDoubleValue() {
        BigDecimalValueTransformer bigDecimalValueTransformer = new BigDecimalValueTransformer();
        Row rowBigDecimal = bigDecimalValueTransformer.transform(null);
        assertEquals(0.0D, rowBigDecimal.getField(4));
        assertTrue(rowBigDecimal.getField(4) instanceof Double);
    }

}
