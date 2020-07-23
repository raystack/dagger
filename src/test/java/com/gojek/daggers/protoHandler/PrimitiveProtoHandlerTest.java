package com.gojek.daggers.protoHandler;

import com.gojek.daggers.exception.InvalidDataTypeException;
import com.gojek.esb.booking.BookingLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrimitiveProtoHandlerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldReturnTrueForAnyDataType() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);

        assertTrue(primitiveProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = primitiveProtoHandler.populateBuilder(builder, null);
        assertEquals("", returnedBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldSetFieldPassedInTheBuilder() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = primitiveProtoHandler.populateBuilder(builder, "123");
        assertEquals("123", returnedBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnIntegerValueForIntegerTypeFieldDescriptorIfIntegerIsPassed() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);

        assertEquals(1, primitiveProtoHandler.transformForPostProcessor(1));
    }

    @Test
    public void shouldReturnIntegerValueForIntegerTypeFieldDescriptorIfIntegerIsPassedAsString() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);

        assertEquals(1, primitiveProtoHandler.transformForPostProcessor("1"));
    }

    @Test
    public void shouldReturnStringValueForStringTypeFieldDescriptorIfStringIsPassed() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(stringFieldDescriptor);

        assertEquals("123", primitiveProtoHandler.transformForPostProcessor("123"));
    }

    @Test
    public void shouldReturnStringValueForStringTypeFieldDescriptorIfStringIsNotPassed() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(stringFieldDescriptor);

        assertEquals("123", primitiveProtoHandler.transformForPostProcessor(123));
    }

    @Test
    public void shouldThrowInvalidDataTypeExceptionInCaseOfTypeMismatch() {
        expectedException.expect(InvalidDataTypeException.class);
        expectedException.expectMessage("type mismatch of field: customer_price, expecting FLOAT type, actual type class java.lang.String");

        Descriptors.FieldDescriptor floatFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("customer_price");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(floatFieldDescriptor);

        primitiveProtoHandler.transformForPostProcessor("stringValue");
    }
}