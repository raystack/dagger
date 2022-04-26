package io.odpf.dagger.common.serde.typehandler.primitive;

import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
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

public class BooleanTypeHandlerTest {
    @Test
    public void shouldHandleBooleanTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        assertTrue(booleanTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanBoolean() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        assertFalse(booleanTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        Object value = booleanTypeHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        Object value = booleanTypeHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeBool() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        Object value = booleanTypeHandler.parseObject(null);

        assertEquals(false, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        assertEquals(Types.BOOLEAN, booleanTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.BOOLEAN), booleanTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        ArrayList<Boolean> inputValues = new ArrayList<>(Arrays.asList(true, false, false));
        Object actualValues = booleanTypeHandler.parseRepeatedObjectField(inputValues);

        assertArrayEquals(new boolean[]{true, false, false}, (boolean[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanTypeHandler booleanTypeHandler = new BooleanTypeHandler(fieldDescriptor);
        Object actualValues = booleanTypeHandler.parseRepeatedObjectField(null);

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

        BooleanTypeHandler booleanHandler = new BooleanTypeHandler(fieldDescriptor);
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
        BooleanTypeHandler booleanHandler = new BooleanTypeHandler(fieldDescriptor);

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
        BooleanTypeHandler booleanHandler = new BooleanTypeHandler(fieldDescriptor);

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

        BooleanTypeHandler booleanHandler = new BooleanTypeHandler(fieldDescriptor);
        boolean[] actualValue = (boolean[]) booleanHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new boolean[]{true, false}, actualValue);
    }

    @Test
    public void shouldReturnEmptyBooleanArrayWhenParseRepeatedSimpleGroupFieldIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("boolean_array_field");

        BooleanTypeHandler booleanHandler = new BooleanTypeHandler(fieldDescriptor);
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

        BooleanTypeHandler booleanHandler = new BooleanTypeHandler(fieldDescriptor);
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

        BooleanTypeHandler booleanHandler = new BooleanTypeHandler(fieldDescriptor);
        boolean[] actualValue = (boolean[]) booleanHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new boolean[0], actualValue);
    }
}
