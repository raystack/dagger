package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NullValueTransformerTest {

    @Test
    public void shouldReturnTrueForNullValues() {
        NullValueTransformer nullValueTransformer = new NullValueTransformer();
        assertEquals(nullValueTransformer.canTransform(null), true);
    }

    @Test
    public void shouldReturnEmptyRow() {
        NullValueTransformer nullValueTransformer = new NullValueTransformer();
        Row row = nullValueTransformer.transform(null);
        Assert.assertEquals(8, row.getArity());
        Assert.assertEquals("+I[null, null, null, null, null, null, null, null]", row.toString());
    }

    @Test(expected = NotImplementedException.class)
    public void shouldThrowExceptionForGetIndex() {
        NullValueTransformer nullValueTransformer = new NullValueTransformer();
        nullValueTransformer.getIndex();
    }

}
