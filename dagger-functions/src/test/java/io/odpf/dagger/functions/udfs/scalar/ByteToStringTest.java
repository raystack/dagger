package io.odpf.dagger.functions.udfs.scalar;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;

public class ByteToStringTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldConvertProtoBytesToString() {
        ByteToString byteToString = new ByteToString();
        ByteString byteString = ByteString.copyFrom("testString".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("testString", byteToString.eval(byteString));
    }

    @Test
    public void shouldConvertProtoBytesToStringForEmptyBytes() {
        ByteToString byteToString = new ByteToString();
        ByteString byteString = ByteString.copyFrom("".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("", byteToString.eval(byteString));
    }

    @Test
    public void shouldThrowNullPointerExceptionIfPassedForNullField() {
        thrown.expect(NullPointerException.class);
        ByteToString byteToString = new ByteToString();
        Assert.assertEquals("", byteToString.eval(null));
    }
}
