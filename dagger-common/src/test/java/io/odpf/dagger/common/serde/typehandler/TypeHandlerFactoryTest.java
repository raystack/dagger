package io.odpf.dagger.common.serde.typehandler;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.serde.typehandler.complex.EnumTypeHandler;
import io.odpf.dagger.common.serde.typehandler.complex.MapTypeHandler;
import io.odpf.dagger.common.serde.typehandler.complex.MessageTypeHandler;
import io.odpf.dagger.common.serde.typehandler.complex.StructMessageTypeHandler;
import io.odpf.dagger.common.serde.typehandler.complex.TimestampTypeHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedEnumTypeHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedMessageTypeHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedPrimitiveTypeHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedStructMessageTypeHandler;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestFeedbackLogMessage;
import io.odpf.dagger.consumer.TestNestedRepeatedMessage;
import io.odpf.dagger.consumer.TestRepeatedEnumMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TypeHandlerFactoryTest {
    @Before
    public void setup() {
        TypeHandlerFactory.clearProtoHandlerMap();
    }

    @Test
    public void shouldReturnMapProtoHandlerIfMapFieldDescriptorPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(mapFieldDescriptor);
        assertEquals(MapTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnTimestampProtoHandlerIfTimestampFieldDescriptorPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(timestampFieldDescriptor);
        assertEquals(TimestampTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnEnumProtoHandlerIfEnumFieldDescriptorPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(enumFieldDescriptor);
        assertEquals(EnumTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedProtoHandlerIfRepeatedFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(repeatedFieldDescriptor);
        assertEquals(RepeatedPrimitiveTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedMessageProtoHandlerIfRepeatedMessageFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor);
        assertEquals(RepeatedMessageTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedEnumProtoHandlerIfRepeatedEnumFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(repeatedEnumFieldDescriptor);
        assertEquals(RepeatedEnumTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedStructProtoHandlerIfRepeatedStructFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(repeatedStructFieldDescriptor);
        assertEquals(RepeatedStructMessageTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnStructProtoHandlerIfStructFieldDescriptorPassed() {
        Descriptors.FieldDescriptor structFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(structFieldDescriptor);
        assertEquals(StructMessageTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnMessageProtoHandlerIfMessageFieldDescriptorPassed() {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(messageFieldDescriptor);
        assertEquals(MessageTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnDefaultProtoHandlerIfPrimitiveFieldDescriptorPassed() {
        Descriptors.FieldDescriptor primitiveFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveTypeHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnTheSameObjectWithMultipleThreads() throws InterruptedException {
        ExecutorService e = Executors.newFixedThreadPool(100);
        final TypeHandler[] cache = {null};
        for (int i = 0; i < 1000; i++) {
            e.submit(() -> {
                Descriptors.FieldDescriptor primitiveFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
                TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
                assertEquals(PrimitiveTypeHandler.class, typeHandler.getClass());
                synchronized (cache) {
                    TypeHandler oldHandler = cache[0];
                    if (oldHandler != null) {
                        assertEquals(typeHandler, cache[0]);
                    } else {
                        // Only one thread will set this
                        cache[0] = typeHandler;
                    }
                }
            });
        }
        e.shutdown();
        e.awaitTermination(10000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void shouldReturnTheSameObjectWhenFactoryMethodIsCalledMultipleTimes() {
        Descriptors.FieldDescriptor primitiveFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveTypeHandler.class, typeHandler.getClass());
        TypeHandler newTypeHandler = TypeHandlerFactory.getProtoHandler(primitiveFieldDescriptor);
        assertEquals(PrimitiveTypeHandler.class, newTypeHandler.getClass());
        assertEquals(typeHandler, newTypeHandler);
    }
}
