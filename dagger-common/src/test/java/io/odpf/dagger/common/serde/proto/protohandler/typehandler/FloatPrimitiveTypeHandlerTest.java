package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FloatPrimitiveTypeHandlerTest {
    @Test
    public void shouldHandleFloatTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(floatPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanFloat() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(floatPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        Object value = floatPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        Object value = floatPrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeFloat() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        Object value = floatPrimitiveTypeHandler.getValue(null);

        assertEquals(0.0f, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.FLOAT, floatPrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.FLOAT), floatPrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        ArrayList<Float> inputValues = new ArrayList<>(Arrays.asList(1F, 2F, 3F));
        Object actualValues = floatPrimitiveTypeHandler.getArray(inputValues);

        assertTrue(Arrays.equals(new float[]{1F, 2F, 3F}, (float[]) actualValues));
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = floatPrimitiveTypeHandler.getArray(null);

        assertEquals(0, ((float[]) actualValues).length);
    }

}
