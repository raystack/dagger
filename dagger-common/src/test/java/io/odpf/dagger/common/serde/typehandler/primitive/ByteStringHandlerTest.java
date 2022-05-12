package io.odpf.dagger.common.serde.typehandler.primitive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestMessageEnvelope;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.*;

public class ByteStringHandlerTest {
    @Test
    public void shouldHandleByteStringTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);
        assertTrue(byteStringHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanByteString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("topic");
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);
        assertFalse(byteStringHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeByteString() {
        ByteString actualValue = ByteString.copyFromUtf8("test");

        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);
        Object value = byteStringHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);
        assertEquals(TypeInformation.of(ByteString.class), byteStringHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);
        assertEquals(Types.OBJECT_ARRAY(TypeInformation.of(ByteString.class)), byteStringHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);
        ArrayList<ByteString> inputValues = new ArrayList<>(Arrays.asList(ByteString.copyFromUtf8("test1"), ByteString.copyFromUtf8("test2")));
        Object actualValues = byteStringHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (ByteString[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);
        Object actualValues = byteStringHandler.getArray(null);
        assertEquals(0, ((ByteString[]) actualValues).length);
    }

    @Test
    public void shouldFetchUTF8EncodedByteStringForFieldOfTypeBinaryInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        String testString = "test-string";
        ByteString expectedByteString = ByteString.copyFrom(testString.getBytes());
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("log_key")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("log_key", Binary.fromConstantByteArray(expectedByteString.toByteArray()));
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);

        Object actualValue = byteStringHandler.parseSimpleGroup(simpleGroup);

        assertEquals(expectedByteString, actualValue);
    }

    @Test
    public void shouldReturnNullIfFieldNotPresentInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);

        Object actualValue = byteStringHandler.parseSimpleGroup(simpleGroup);

        assertNull(actualValue);
    }

    @Test
    public void shouldReturnNullIfFieldNotInitializedWithAValueInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("log_key")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        ByteStringHandler byteStringHandler = new ByteStringHandler(fieldDescriptor);

        Object actualValue = byteStringHandler.parseSimpleGroup(simpleGroup);

        assertNull(actualValue);
    }
}
