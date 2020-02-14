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

public class DefaultProtoHandlerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldReturnTrueForAnyDataType() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        DefaultProtoHandler defaultProtoHandler = new DefaultProtoHandler(fieldDescriptor);

        assertTrue(defaultProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        DefaultProtoHandler defaultProtoHandler = new DefaultProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = defaultProtoHandler.getProtoBuilder(builder, null);
        assertEquals(builder, returnedBuilder);
    }

    @Test
    public void shouldSetFieldPassedInTheBuilder() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        DefaultProtoHandler defaultProtoHandler = new DefaultProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = defaultProtoHandler.getProtoBuilder(builder, "123");
        assertEquals("123", returnedBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnIntegerValueForIntegerTypeFieldDescriptorIfIntegerIsPassed() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");
        DefaultProtoHandler defaultProtoHandler = new DefaultProtoHandler(fieldDescriptor);

        assertEquals(1, defaultProtoHandler.getTypeAppropriateValue(1));
    }

    @Test
    public void shouldReturnIntegerValueForIntegerTypeFieldDescriptorIfIntegerIsPassedAsString() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");
        DefaultProtoHandler defaultProtoHandler = new DefaultProtoHandler(fieldDescriptor);

        assertEquals(1, defaultProtoHandler.getTypeAppropriateValue("1"));
    }

    @Test
    public void shouldReturnStringValueForStringTypeFieldDescriptorIfStringIsPassed() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        DefaultProtoHandler defaultProtoHandler = new DefaultProtoHandler(stringFieldDescriptor);

        assertEquals("123", defaultProtoHandler.getTypeAppropriateValue("123"));
    }

    @Test
    public void shouldReturnStringValueForStringTypeFieldDescriptorIfStringIsNotPassed() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        DefaultProtoHandler defaultProtoHandler = new DefaultProtoHandler(stringFieldDescriptor);

        assertEquals("123", defaultProtoHandler.getTypeAppropriateValue(123));
    }

    @Test
    public void shouldThrowInvalidDataTypeExceptionInCaseOfTypeMismatch() {
        expectedException.expect(InvalidDataTypeException.class);
        expectedException.expectMessage("type mismatch of field: customer_price, expecting FLOAT type, actual type class java.lang.String");

        Descriptors.FieldDescriptor floatFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("customer_price");
        DefaultProtoHandler defaultProtoHandler = new DefaultProtoHandler(floatFieldDescriptor);

        defaultProtoHandler.getTypeAppropriateValue("stringValue");
    }
}