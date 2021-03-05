package com.gojek.daggers.protohandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.booking.GoLifeBookingLogMessage;
import com.gojek.esb.clevertap.ClevertapEventLogMessage;
import com.gojek.esb.consumer.TestNestedRepeatedMessage;
import com.gojek.esb.consumer.TestRepeatedEnumMessage;
import com.gojek.esb.customersanction.CustomerSanctionLogMessage;
import com.google.protobuf.Descriptors;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ProtoHandlerFactoryTest {
    @Before
    public void setup() {
        ProtoHandlerFactory.clearProtoHandlerMap();
    }

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
    public void shouldReturnRepeatedEnumProtoHandlerIfRepeatedEnumFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(repeatedEnumFieldDescriptor);
        assertEquals(RepeatedEnumProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedStructProtoHandlerIfRepeatedStructFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(repeatedStructFieldDescriptor);
        assertEquals(RepeatedStructMessageProtoHandler.class, protoHandler.getClass());
    }

    @Test
    public void shouldReturnStructProtoHandlerIfStructFieldDescriptorPassed() {
        Descriptors.FieldDescriptor structFieldDescriptor = ClevertapEventLogMessage.getDescriptor().findFieldByName("profile_data");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(structFieldDescriptor);
        assertEquals(StructMessageProtoHandler.class, protoHandler.getClass());
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

    @Test
    public void shouldReturnTheSameObjectWithMultipleThreads() throws InterruptedException {
        ExecutorService e = Executors.newFixedThreadPool(100);
        final ProtoHandler[] cache = {null};
        for (int i = 0; i < 1000; i++) {
            e.submit(() -> {
                Descriptors.FieldDescriptor primitiveFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("order_number");
                ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
                assertEquals(PrimitiveProtoHandler.class, protoHandler.getClass());
                synchronized (cache) {
                    ProtoHandler oldHandler = cache[0];
                    if (oldHandler != null) {
                        assertEquals(protoHandler, cache[0]);
                    } else {
                        // Only one thread will set this
                        cache[0] = protoHandler;
                    }
                }
            });
        }
        e.shutdown();
        e.awaitTermination(10000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void shouldReturnTheSameObjectWhenFactoryMethodIsCalledMultipleTimes() {
        Descriptors.FieldDescriptor primitiveFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("order_number");
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveProtoHandler.class, protoHandler.getClass());
        ProtoHandler newProtoHandler = ProtoHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveProtoHandler.class, newProtoHandler.getClass());
        assertEquals(protoHandler, newProtoHandler);
    }
}
