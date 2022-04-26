package io.odpf.dagger.common.serde.typehandler.primitive;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringTypeHandlerTest {

    @Test
    public void shouldHandleStringTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        assertTrue(stringTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        assertFalse(stringTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeString() {
        String actualValue = "test";

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        Object value = stringTypeHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeString() {
        Integer actualValue = 23;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        Object value = stringTypeHandler.parseObject(actualValue);

        assertEquals("23", value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        Object value = stringTypeHandler.parseObject(null);

        assertEquals("", value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        assertEquals(Types.STRING, stringTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        assertEquals(ObjectArrayTypeInfo.getInfoFor(Types.STRING), stringTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>(Arrays.asList("1", "2", "3"));
        Object actualValues = stringTypeHandler.parseObjectArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (String[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringTypeHandler stringTypeHandler = new StringTypeHandler(fieldDescriptor);
        Object actualValues = stringTypeHandler.parseObjectArray(null);
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
        StringTypeHandler stringHandler = new StringTypeHandler(fieldDescriptor);

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
        StringTypeHandler stringHandler = new StringTypeHandler(fieldDescriptor);

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
        StringTypeHandler stringHandler = new StringTypeHandler(fieldDescriptor);

        Object actualValue = stringHandler.parseSimpleGroup(simpleGroup);

        assertEquals("", actualValue);
    }
}
