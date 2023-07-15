package org.raystack.dagger.common.serde.typehandler.primitive;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.protobuf.Descriptors;
import org.raystack.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringHandlerTest {

    @Test
    public void shouldHandleStringTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        assertTrue(stringHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        assertFalse(stringHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeString() {
        String actualValue = "test";

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        Object value = stringHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeString() {
        Integer actualValue = 23;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        Object value = stringHandler.parseObject(actualValue);

        assertEquals("23", value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        Object value = stringHandler.parseObject(null);

        assertEquals("", value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        assertEquals(Types.STRING, stringHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        assertEquals(ObjectArrayTypeInfo.getInfoFor(Types.STRING), stringHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>(Arrays.asList("1", "2", "3"));
        Object actualValues = stringHandler.parseRepeatedObjectField(inputValues);
        assertArrayEquals(inputValues.toArray(), (String[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        Object actualValues = stringHandler.parseRepeatedObjectField(null);
        assertEquals(0, ((String[]) actualValues).length);
    }

    @Test
    public void shouldFetchParsedValueForFieldOfTypeStringInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("order_number")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("order_number", "some-value");
        StringHandler stringHandler = new StringHandler(fieldDescriptor);

        Object actualValue = stringHandler.parseSimpleGroup(simpleGroup);

        assertEquals("some-value", actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotPresentInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        StringHandler stringHandler = new StringHandler(fieldDescriptor);

        Object actualValue = stringHandler.parseSimpleGroup(simpleGroup);

        assertEquals("", actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotInitializedWithAValueInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("order_number")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        StringHandler stringHandler = new StringHandler(fieldDescriptor);

        Object actualValue = stringHandler.parseSimpleGroup(simpleGroup);

        assertEquals("", actualValue);
    }

    @Test
    public void shouldReturnArrayOfStringValuesForFieldOfTypeRepeatedBinaryInsideSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");

        GroupType parquetSchema = buildMessage()
                .repeated(BINARY).named("meta_array")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        simpleGroup.add("meta_array", "Hello World");
        simpleGroup.add("meta_array", "Welcome");

        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        String[] actualValue = (String[]) stringHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new String[]{"Hello World", "Welcome"}, actualValue);
    }

    @Test
    public void shouldReturnEmptyStringArrayWhenParseRepeatedSimpleGroupFieldIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");

        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        String[] actualValue = (String[]) stringHandler.parseRepeatedSimpleGroupField(null);

        assertArrayEquals(new String[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyStringArrayWhenRepeatedBinaryFieldInsideSimpleGroupIsNotPresent() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("some_other_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        String[] actualValue = (String[]) stringHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new String[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyStringArrayWhenRepeatedBinaryFieldInsideSimpleGroupIsNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");

        GroupType parquetSchema = buildMessage()
                .repeated(BINARY).named("meta_array")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        StringHandler stringHandler = new StringHandler(fieldDescriptor);
        String[] actualValue = (String[]) stringHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new String[0], actualValue);
    }
}
