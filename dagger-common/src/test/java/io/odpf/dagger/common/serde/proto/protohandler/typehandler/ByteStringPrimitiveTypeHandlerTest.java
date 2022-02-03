package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestMessageEnvelope;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ByteStringPrimitiveTypeHandlerTest {
    @Test
    public void shouldHandleByteStringTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringPrimitiveTypeHandler byteStringPrimitiveTypeHandler = new ByteStringPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(byteStringPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanByteString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("topic");
        ByteStringPrimitiveTypeHandler byteStringPrimitiveTypeHandler = new ByteStringPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(byteStringPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeByteString() {
        ByteString actualValue = ByteString.copyFromUtf8("test");

        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringPrimitiveTypeHandler byteStringPrimitiveTypeHandler = new ByteStringPrimitiveTypeHandler(fieldDescriptor);
        Object value = byteStringPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringPrimitiveTypeHandler byteStringPrimitiveTypeHandler = new ByteStringPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(TypeInformation.of(ByteString.class), byteStringPrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringPrimitiveTypeHandler byteStringPrimitiveTypeHandler = new ByteStringPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.OBJECT_ARRAY(TypeInformation.of(ByteString.class)), byteStringPrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringPrimitiveTypeHandler byteStringPrimitiveTypeHandler = new ByteStringPrimitiveTypeHandler(fieldDescriptor);
        ArrayList<ByteString> inputValues = new ArrayList<>(Arrays.asList(ByteString.copyFromUtf8("test1"), ByteString.copyFromUtf8("test2")));
        Object actualValues = byteStringPrimitiveTypeHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (ByteString[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringPrimitiveTypeHandler byteStringPrimitiveTypeHandler = new ByteStringPrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = byteStringPrimitiveTypeHandler.getArray(null);
        assertEquals(0, ((ByteString[]) actualValues).length);
    }

}
