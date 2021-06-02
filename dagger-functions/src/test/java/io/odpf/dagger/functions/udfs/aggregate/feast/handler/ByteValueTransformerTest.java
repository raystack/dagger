package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import com.google.protobuf.ByteString;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteValueTransformerTest {

    @Test
    public void shouldReturnTrueForByteValues() {
        ByteValueTransformer byteValueHandler = new ByteValueTransformer();
        assertEquals(byteValueHandler.canTransform(ByteString.copyFrom("value1".getBytes())), true);
    }

    @Test
    public void shouldReturnValue() {
        ByteValueTransformer byteValueHandler = new ByteValueTransformer();
        Row byteRow = byteValueHandler.transform(ByteString.copyFrom("value1".getBytes()));
        assertEquals(ByteString.copyFrom("value1".getBytes()), byteRow.getField(0));
    }

    @Test
    public void shouldReturnDefaultValueForNull() {
        ByteValueTransformer byteValueHandler = new ByteValueTransformer();
        Row byteRow = byteValueHandler.transform(null);
        assertEquals(null, byteRow.getField(0));
    }
}
