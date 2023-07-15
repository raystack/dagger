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
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.Types.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FloatHandlerTest {
    @Test
    public void shouldHandleFloatTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        assertTrue(floatHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanFloat() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        assertFalse(floatHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        Object value = floatHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        Object value = floatHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeFloat() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        Object value = floatHandler.parseObject(null);

        assertEquals(0.0f, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        assertEquals(Types.FLOAT, floatHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.FLOAT), floatHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        ArrayList<Float> inputValues = new ArrayList<>(Arrays.asList(1F, 2F, 3F));
        Object actualValues = floatHandler.parseRepeatedObjectField(inputValues);

        assertTrue(Arrays.equals(new float[]{1F, 2F, 3F}, (float[]) actualValues));
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        Object actualValues = floatHandler.parseRepeatedObjectField(null);

        assertEquals(0, ((float[]) actualValues).length);
    }

    @Test
    public void shouldFetchParsedValueForFieldOfTypeFloatInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        GroupType parquetSchema = requiredGroup()
                .required(FLOAT).named("amount_paid_by_cash")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("amount_paid_by_cash", 32.56F);
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);

        Object actualValue = floatHandler.parseSimpleGroup(simpleGroup);

        assertEquals(32.56F, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotPresentInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        GroupType parquetSchema = requiredGroup()
                .required(FLOAT).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);

        Object actualValue = floatHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0.0F, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotInitializedWithAValueInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");

        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = requiredGroup()
                .required(FLOAT).named("amount_paid_by_cash")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);

        Object actualValue = floatHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0.0F, actualValue);
    }

    @Test
    public void shouldReturnArrayOfFloatValuesForFieldOfTypeRepeatedFloatInsideSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("float_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(FLOAT).named("float_array_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        simpleGroup.add("float_array_field", 0.45123F);
        simpleGroup.add("float_array_field", 23.0123F);

        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        float[] actualValue = (float[]) floatHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new float[]{0.45123F, 23.0123F}, actualValue, 0F);
    }

    @Test
    public void shouldReturnEmptyFloatArrayWhenParseRepeatedSimpleGroupFieldIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("float_array_field");

        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        float[] actualValue = (float[]) floatHandler.parseRepeatedSimpleGroupField(null);

        assertArrayEquals(new float[0], actualValue, 0F);
    }

    @Test
    public void shouldReturnEmptyFloatArrayWhenRepeatedFloatFieldInsideSimpleGroupIsNotPresent() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("float_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("some_other_array_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        float[] actualValue = (float[]) floatHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new float[0], actualValue, 0F);
    }

    @Test
    public void shouldReturnEmptyFloatArrayWhenRepeatedFloatFieldInsideSimpleGroupIsNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("float_array_field");

        GroupType parquetSchema = buildMessage()
                .repeated(FLOAT).named("float_array_field")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        FloatHandler floatHandler = new FloatHandler(fieldDescriptor);
        float[] actualValue = (float[]) floatHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new float[0], actualValue, 0F);
    }

}
