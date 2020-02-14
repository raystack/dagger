package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.*;

public class FloatPrimitiveTypeHandlerTest {
    @Test
    public void shouldHandleFloatTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("total_unsubsidised_price");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(floatPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanFloat() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(floatPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("total_unsubsidised_price");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        Object value = floatPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeFloat() {
        float actualValue = 5.1f;

        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("total_unsubsidised_price");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        Object value = floatPrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeFloat() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("total_unsubsidised_price");
        FloatPrimitiveTypeHandler floatPrimitiveTypeHandler = new FloatPrimitiveTypeHandler(fieldDescriptor);
        Object value = floatPrimitiveTypeHandler.getValue(null);

        assertEquals(0.0f, value);
    }

}