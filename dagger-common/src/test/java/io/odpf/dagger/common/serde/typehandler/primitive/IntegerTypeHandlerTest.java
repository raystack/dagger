package io.odpf.dagger.common.serde.typehandler.primitive;

import io.odpf.dagger.consumer.TestNestedRepeatedMessage;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntegerTypeHandlerTest {

    @Test
    public void shouldHandleIntegerTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        assertTrue(integerTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanInteger() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        assertFalse(integerTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeInteger() {
        int actualValue = 2;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        Object value = integerTypeHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeInteger() {
        int actualValue = 2;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        Object value = integerTypeHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeInteger() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        Object value = integerTypeHandler.parseObject(null);

        assertEquals(0, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        assertEquals(Types.INT, integerTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.INT), integerTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        ArrayList<Integer> inputValues = new ArrayList<>(Arrays.asList(1, 2, 3));
        Object actualValues = integerTypeHandler.parseRepeatedObjectField(inputValues);

        assertArrayEquals(new int[]{1, 2, 3}, (int[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id");
        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        Object actualValues = integerTypeHandler.parseRepeatedObjectField(null);

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

        IntegerTypeHandler integerHandler = new IntegerTypeHandler(fieldDescriptor);
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
        IntegerTypeHandler integerHandler = new IntegerTypeHandler(fieldDescriptor);

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
        IntegerTypeHandler integerHandler = new IntegerTypeHandler(fieldDescriptor);

        Object actualValue = integerHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0, actualValue);
    }

    @Test
    public void shouldReturnArrayOfIntValuesForFieldOfTypeRepeatedInt32InsideSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_number_field");

        GroupType parquetSchema = buildMessage()
                .repeated(INT32).named("repeated_number_field")
                .named("TestNestedRepeatedMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        simpleGroup.add("repeated_number_field", 2342882);
        simpleGroup.add("repeated_number_field", -382922);

        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        int[] actualValue = (int[]) integerTypeHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new int[]{2342882, -382922}, actualValue);
    }

    @Test
    public void shouldReturnEmptyIntArrayWhenParseRepeatedSimpleGroupFieldIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_number_field");

        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        int[] actualValue = (int[]) integerTypeHandler.parseRepeatedSimpleGroupField(null);

        assertArrayEquals(new int[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyIntArrayWhenRepeatedInt32FieldInsideSimpleGroupIsNotPresent() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_number_field");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("some_other_field")
                .named("TestNestedRepeatedMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        int[] actualValue = (int[]) integerTypeHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new int[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyIntArrayWhenRepeatedInt32FieldInsideSimpleGroupIsNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_number_field");

        GroupType parquetSchema = buildMessage()
                .repeated(INT32).named("repeated_number_field")
                .named("TestNestedRepeatedMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        IntegerTypeHandler integerTypeHandler = new IntegerTypeHandler(fieldDescriptor);
        int[] actualValue = (int[]) integerTypeHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new int[0], actualValue);
    }
}
