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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BooleanPrimitiveHandlerTest {
    @Test
    public void shouldHandleBooleanTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        assertTrue(booleanPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanBoolean() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        assertFalse(booleanPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        Object value = booleanPrimitiveHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        Object value = booleanPrimitiveHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeBool() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        Object value = booleanPrimitiveHandler.parseObject(null);

        assertEquals(false, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        assertEquals(Types.BOOLEAN, booleanPrimitiveHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");
        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.BOOLEAN), booleanPrimitiveHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        ArrayList<Boolean> inputValues = new ArrayList<>(Arrays.asList(true, false, false));
        Object actualValues = booleanPrimitiveHandler.getArray(inputValues);

        assertArrayEquals(new boolean[]{true, false, false}, (boolean[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled");

        BooleanPrimitiveHandler booleanPrimitiveHandler = new BooleanPrimitiveHandler(fieldDescriptor);
        Object actualValues = booleanPrimitiveHandler.getArray(null);

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

        BooleanPrimitiveHandler booleanHandler = new BooleanPrimitiveHandler(fieldDescriptor);
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
        BooleanPrimitiveHandler booleanHandler = new BooleanPrimitiveHandler(fieldDescriptor);

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
        BooleanPrimitiveHandler booleanHandler = new BooleanPrimitiveHandler(fieldDescriptor);

        Object actualValue = booleanHandler.parseSimpleGroup(simpleGroup);

        assertEquals(false, actualValue);
    }
}
