package io.odpf.dagger.common.serde.typehandler.primitive;

import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.Types.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FloatPrimitiveHandlerTest {
    @Test
    public void shouldHandleFloatTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        assertTrue(floatPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanFloat() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        assertFalse(floatPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        Object value = floatPrimitiveHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        Object value = floatPrimitiveHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeFloat() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        Object value = floatPrimitiveHandler.parseObject(null);

        assertEquals(0.0f, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        assertEquals(Types.FLOAT, floatPrimitiveHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.FLOAT), floatPrimitiveHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        ArrayList<Float> inputValues = new ArrayList<>(Arrays.asList(1F, 2F, 3F));
        Object actualValues = floatPrimitiveHandler.getArray(inputValues);

        assertTrue(Arrays.equals(new float[]{1F, 2F, 3F}, (float[]) actualValues));
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveHandler floatPrimitiveHandler = new FloatPrimitiveHandler(fieldDescriptor);
        Object actualValues = floatPrimitiveHandler.getArray(null);

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
        FloatPrimitiveHandler floatHandler = new FloatPrimitiveHandler(fieldDescriptor);

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
        FloatPrimitiveHandler floatHandler = new FloatPrimitiveHandler(fieldDescriptor);

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
        FloatPrimitiveHandler floatHandler = new FloatPrimitiveHandler(fieldDescriptor);

        Object actualValue = floatHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0.0F, actualValue);
    }

}
