package com.gojek.daggers.protoHandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.booking.PaymentOptionMetadata;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class MessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor messageFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageProtoHandler messsageProtoHandler = new MessageProtoHandler(messageFieldDescriptor);

        assertTrue(messsageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanMessageTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        MessageProtoHandler messsageProtoHandler = new MessageProtoHandler(otherFieldDescriptor);

        assertFalse(messsageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        MessageProtoHandler messsageProtoHandler = new MessageProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals(builder, messsageProtoHandler.getProtoBuilder(builder, 123));
        assertEquals("", messsageProtoHandler.getProtoBuilder(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForMessageFieldTypeDescriptorIfAllFieldsPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor messageFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageProtoHandler messageProtoHandler = new MessageProtoHandler(messageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageFieldDescriptor.getContainingType());

        Row inputRow = new Row(2);
        inputRow.setField(0, "test1");
        inputRow.setField(1, "test2");
        DynamicMessage.Builder returnedBuilder = messageProtoHandler.getProtoBuilder(builder, inputRow);

        PaymentOptionMetadata returnedValue = PaymentOptionMetadata.parseFrom(((DynamicMessage) returnedBuilder.getField(messageFieldDescriptor)).toByteArray());

        assertEquals("test1", returnedValue.getMaskedCard());
        assertEquals("test2", returnedValue.getNetwork());
    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForMessageFieldTypeDescriptorIfAllFieldsAreNotPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor messageFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageProtoHandler messageProtoHandler = new MessageProtoHandler(messageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageFieldDescriptor.getContainingType());

        Row inputRow = new Row(1);
        inputRow.setField(0, "test1");
        DynamicMessage.Builder returnedBuilder = messageProtoHandler.getProtoBuilder(builder, inputRow);

        PaymentOptionMetadata returnedValue = PaymentOptionMetadata.parseFrom(((DynamicMessage) returnedBuilder.getField(messageFieldDescriptor)).toByteArray());

        assertEquals("test1", returnedValue.getMaskedCard());
        assertEquals("", returnedValue.getNetwork());
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueArePassed() {
        HashMap<String, String> inputValues = new HashMap<>();
        inputValues.put("masked_card", "test1");
        inputValues.put("network", "test2");

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) ProtoHandlerFactory.getProtoHandler(fieldDescriptor).getTypeAppropriateValue(inputValues);

        assertEquals("test1", value.getField(0));
        assertEquals("test2", value.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueAreNotPassed() {
        HashMap<String, String> inputValues = new HashMap<>();
        inputValues.put("masked_card", "test1");

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) ProtoHandlerFactory.getProtoHandler(fieldDescriptor).getTypeAppropriateValue(inputValues);

        assertEquals("test1", value.getField(0));
        assertEquals(null, value.getField(1));
    }

    @Test
    public void shouldReturnEmptyRowIfNullPassed() {
        HashMap<String, String> inputValues = new HashMap<>();
        inputValues.put("masked_card", "test1");

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) ProtoHandlerFactory.getProtoHandler(fieldDescriptor).getTypeAppropriateValue(null);

        assertEquals(2, value.getArity());
        assertEquals(null, value.getField(0));
        assertEquals(null, value.getField(1));
    }
}