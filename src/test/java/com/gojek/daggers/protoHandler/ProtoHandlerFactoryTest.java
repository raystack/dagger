package com.gojek.daggers.protoHandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.booking.GoLifeBookingLogMessage;
import com.gojek.esb.customersanction.CustomerSanctionLogMessage;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProtoHandlerFactoryTest {

    @Test
    public void shouldReturnMapProtoHandlerIfMapFieldDescriptorPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = CustomerSanctionLogMessage.getDescriptor().findFieldByName("metadata");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(mapFieldDescriptor);
        assertEquals(MapProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnTimestampProtoHandlerIfTimestampFieldDescriptorPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = CustomerSanctionLogMessage.getDescriptor().findFieldByName("event_timestamp");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(timestampFieldDescriptor);
        assertEquals(TimestampProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnEnumProtoHandlerIfEnumFieldDescriptorPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("service_type");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(enumFieldDescriptor);
        assertEquals(EnumProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedProtoHandlerIfRepeatedFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(repeatedFieldDescriptor);
        assertEquals(RepeatedPrimitiveProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedMessageProtoHandlerIfRepeatedMessageFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("routes");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor);
        assertEquals(RepeatedMessageProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnMessageProtoHandlerIfMessageFieldDescriptorPassed() {
        Descriptors.FieldDescriptor messageFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("driver_eta_pickup");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(messageFieldDescriptor);
        assertEquals(MessageProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnDefaultProtoHandlerIfPrimitiveFieldDescriptorPassed() {
        Descriptors.FieldDescriptor primitiveFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("order_number");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveProtoHandler.class, protoHandler.getClass());
    }

}