package com.gojek.daggers.protoHandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.*;

public class TimestampProtoHandlerTest {
    @Test
    public void shouldReturnTrueIfTimestampFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);

        assertTrue(timestampProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(otherFieldDescriptor);

        assertFalse(timestampProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderIfCannotHandle() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = timestampProtoHandler.getProtoBuilder(builder, "123");
        assertEquals(builder, returnedBuilder);
    }

    @Test
    public void shouldReturnSameBuilderIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = timestampProtoHandler.getProtoBuilder(builder, null);
        assertEquals(builder, returnedBuilder);
    }

    @Test
    public void shouldSetTimestampIfInstanceOfJavaSqlTimestampPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long milliSeconds = System.currentTimeMillis();

        Timestamp inputTimestamp = new Timestamp(milliSeconds);
        DynamicMessage dynamicMessage = timestampProtoHandler.getProtoBuilder(builder, inputTimestamp).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(milliSeconds / 1000, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(inputTimestamp.getNanos(), bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldSetTimestampIfRowHavingTimestampIsPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long seconds = System.currentTimeMillis() / 1000;
        int nanos = (int) (System.currentTimeMillis() * 1000000);

        Row inputRow = new Row(2);
        inputRow.setField(0, seconds);
        inputRow.setField(1, nanos);

        DynamicMessage dynamicMessage = timestampProtoHandler.getProtoBuilder(builder, inputRow).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(seconds, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(nanos, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldThrowExceptionIfRowOfArityOtherThanTwoIsPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        Row inputRow = new Row(3);

        try {
            timestampProtoHandler.getProtoBuilder(builder, inputRow).build();
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Row: null,null,null of size: 3 cannot be converted to timestamp", e.getMessage());
        }
    }

    @Test
    public void shouldSetTimestampIfInstanceOfNumberPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        long seconds = System.currentTimeMillis() / 1000;

        DynamicMessage dynamicMessage = timestampProtoHandler.getProtoBuilder(builder, seconds).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(seconds, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(0, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldSetTimestampIfInstanceOfStringPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor timestampFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TimestampProtoHandler timestampProtoHandler = new TimestampProtoHandler(timestampFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(timestampFieldDescriptor.getContainingType());

        String inputTimestamp = "2019-03-28T05:50:13Z";

        DynamicMessage dynamicMessage = timestampProtoHandler.getProtoBuilder(builder, inputTimestamp).build();

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(dynamicMessage.toByteArray());
        assertEquals(1553752213, bookingLogMessage.getEventTimestamp().getSeconds());
        assertEquals(0, bookingLogMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldFetchTimeStampAsStringFromFieldForFieldDescriptorOfTypeTimeStamp() {
        String actualValue = "2018-08-30T02:21:39.975107Z";

        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.getTypeAppropriateValue(actualValue);
        assertEquals(actualValue, value);
    }

    @Test
    public void shouldReturnNullWhenTimeStampNotAvailableAndFieldDescriptorOfTypeTimeStamp() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("booking_creation_time");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.getTypeAppropriateValue(null);
        assertNull(value);
    }

    @Test
    public void shouldHandleTimestampMessagesByReturningNullForNonParseableTimeStamps() {
        Descriptors.Descriptor descriptor = BookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("event_timestamp");

        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);

        Object value = protoHandler.getTypeAppropriateValue("2");

        assertNull(value);
    }

}