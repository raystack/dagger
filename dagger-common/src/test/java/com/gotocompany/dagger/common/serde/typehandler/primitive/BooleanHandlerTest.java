package org.raystack.dagger.common.serde.typehandler.primitive;

import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import org.raystack.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BooleanHandlerTest {
    @Test
    public void shouldHandleBooleanTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        assertTrue(booleanHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanBoolean() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        assertFalse(booleanHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        Object value = booleanHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        Object value = booleanHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeBool() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        Object value = booleanHandler.parseObject(null);

        assertEquals(false, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        assertEquals(Types.BOOLEAN, booleanHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.BOOLEAN), booleanHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        ArrayList<Boolean> inputValues = new ArrayList<>(Arrays.asList(true, false, false));
        Object actualValues = booleanHandler.parseRepeatedObjectField(inputValues);

        assertArrayEquals(new boolean[]{true, false, false}, (boolean[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        Object actualValues = booleanHandler.parseRepeatedObjectField(null);

        assertEquals(0, ((boolean[]) actualValues).length);
    }

    @Test
    public void shouldFetchParsedValueForFieldOfTypeBoolInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BOOLEAN).named("customer_dynamic_surge_enabled")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("customer_dynamic_surge_enabled", true);

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        Object actualValue = booleanHandler.parseSimpleGroup(simpleGroup);

        assertEquals(true, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotPresentInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BOOLEAN).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);

        Object actualValue = booleanHandler.parseSimpleGroup(simpleGroup);

        assertEquals(false, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotInitializedWithAValueInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BOOLEAN).named("customer_dynamic_surge_enabled")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);

        Object actualValue = booleanHandler.parseSimpleGroup(simpleGroup);

        assertEquals(false, actualValue);
    }

    @Test
    public void shouldReturnArrayOfJavaBooleanValuesForFieldOfTypeRepeatedBooleanInsideSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("boolean_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("boolean_array_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("boolean_array_field", true);
        simpleGroup.add("boolean_array_field", false);

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        boolean[] actualValue = (boolean[]) booleanHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new boolean[]{true, false}, actualValue);
    }

    @Test
    public void shouldReturnEmptyBooleanArrayWhenParseRepeatedSimpleGroupFieldIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("boolean_array_field");

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        boolean[] actualValue = (boolean[]) booleanHandler.parseRepeatedSimpleGroupField(null);

        assertArrayEquals(new boolean[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyBooleanArrayWhenRepeatedFieldInsideSimpleGroupIsNotPresent() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("boolean_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("some_other_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        boolean[] actualValue = (boolean[]) booleanHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new boolean[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyBooleanArrayWhenRepeatedFieldInsideSimpleGroupIsNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("boolean_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("boolean_array_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        BooleanHandler booleanHandler = new BooleanHandler(fieldDescriptor);
        boolean[] actualValue = (boolean[]) booleanHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new boolean[0], actualValue);
    }
}
