package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.types.GoFoodShoppingItemProto;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.*;

public class BooleanPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleBooleanTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("is_reblast");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(booleanPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanBoolean() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(booleanPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");
        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeBool() {
        boolean actualValue = true;

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeBool() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("is_reblast");

        BooleanPrimitiveTypeHandler booleanPrimitiveTypeHandler = new BooleanPrimitiveTypeHandler(fieldDescriptor);
        Object value = booleanPrimitiveTypeHandler.getValue(null);

        assertEquals(false, value);
    }

}