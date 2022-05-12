package io.odpf.dagger.common.serde.typehandler.primitive;

import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntegerHandlerTest {

    @Test
    public void shouldHandleIntegerTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        assertTrue(integerHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanInteger() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        assertFalse(integerHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeInteger() {
        int actualValue = 2;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        Object value = integerHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeInteger() {
        int actualValue = 2;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        Object value = integerHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeInteger() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        Object value = integerHandler.parseObject(null);

        assertEquals(0, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        assertEquals(Types.INT, integerHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.INT), integerHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        ArrayList<Integer> inputValues = new ArrayList<>(Arrays.asList(1, 2, 3));
        Object actualValues = integerHandler.getArray(inputValues);

        assertArrayEquals(new int[]{1, 2, 3}, (int[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        Object actualValues = integerHandler.getArray(null);

        assertEquals(0, ((int[]) actualValues).length);
    }

    @Test
    public void shouldFetchParsedValueForFieldOfTypeIntegerInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");

        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT32).named("cancel_reason_id")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("cancel_reason_id", 34);

        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);
        Object actualValue = integerHandler.parseSimpleGroup(simpleGroup);

        assertEquals(34, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotPresentInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");

        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT32).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);

        Object actualValue = integerHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotInitializedWithAValueInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");

        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT32).named("cancel_reason_id")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        IntegerHandler integerHandler = new IntegerHandler(fieldDescriptor);

        Object actualValue = integerHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0, actualValue);
    }
}
