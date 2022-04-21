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

public class StringPrimitiveHandlerTest {

    @Test
    public void shouldHandleStringTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        assertTrue(stringPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        assertFalse(stringPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeString() {
        String actualValue = "test";

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        Object value = stringPrimitiveHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeString() {
        Integer actualValue = 23;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        Object value = stringPrimitiveHandler.parseObject(actualValue);

        assertEquals("23", value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        Object value = stringPrimitiveHandler.parseObject(null);

        assertEquals("", value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        assertEquals(Types.STRING, stringPrimitiveHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        assertEquals(ObjectArrayTypeInfo.getInfoFor(Types.STRING), stringPrimitiveHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>(Arrays.asList("1", "2", "3"));
        Object actualValues = stringPrimitiveHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (String[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        StringPrimitiveHandler stringPrimitiveHandler = new StringPrimitiveHandler(fieldDescriptor);
        Object actualValues = stringPrimitiveHandler.getArray(null);
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
        StringPrimitiveHandler stringHandler = new StringPrimitiveHandler(fieldDescriptor);

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
        StringPrimitiveHandler stringHandler = new StringPrimitiveHandler(fieldDescriptor);

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
        StringPrimitiveHandler stringHandler = new StringPrimitiveHandler(fieldDescriptor);

        Object actualValue = stringHandler.parseSimpleGroup(simpleGroup);

        assertEquals("", actualValue);
    }
}
