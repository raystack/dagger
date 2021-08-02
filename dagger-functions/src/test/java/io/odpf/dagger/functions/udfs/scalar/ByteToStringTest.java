package io.odpf.dagger.functions.udfs.scalar;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class ByteToStringTest {

    @Before
    public void setup() {
    }

    @Test
    public void shouldConvertProtoBytesToString() {
        ByteToString byteToString = new ByteToString();
        ByteString byteString = ByteString.copyFrom("testString".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("testString", byteToString.eval(byteString));
    }
}
