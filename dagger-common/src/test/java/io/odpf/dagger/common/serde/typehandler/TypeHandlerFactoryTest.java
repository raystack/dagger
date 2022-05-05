package io.odpf.dagger.common.serde.typehandler;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.serde.typehandler.complex.EnumHandler;
import io.odpf.dagger.common.serde.typehandler.complex.MapHandler;
import io.odpf.dagger.common.serde.typehandler.complex.MessageHandler;
import io.odpf.dagger.common.serde.typehandler.complex.StructMessageHandler;
import io.odpf.dagger.common.serde.typehandler.complex.TimestampHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedEnumHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedMessageHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedPrimitiveHandler;
import io.odpf.dagger.common.serde.typehandler.repeated.RepeatedStructMessageHandler;
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
        assertEquals(MapHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnTimestampProtoHandlerIfTimestampFieldDescriptorPassed() {
        Descriptors.FieldDescriptor timestampFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("event_timestamp");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(timestampFieldDescriptor);
        assertEquals(TimestampHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnEnumProtoHandlerIfEnumFieldDescriptorPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(enumFieldDescriptor);
        assertEquals(EnumHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedProtoHandlerIfRepeatedFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(repeatedFieldDescriptor);
        assertEquals(RepeatedPrimitiveHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedMessageProtoHandlerIfRepeatedMessageFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor);
        assertEquals(RepeatedMessageHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedEnumProtoHandlerIfRepeatedEnumFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(repeatedEnumFieldDescriptor);
        assertEquals(RepeatedEnumHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnRepeatedStructProtoHandlerIfRepeatedStructFieldDescriptorPassed() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(repeatedStructFieldDescriptor);
        assertEquals(RepeatedStructMessageHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnStructProtoHandlerIfStructFieldDescriptorPassed() {
        Descriptors.FieldDescriptor structFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(structFieldDescriptor);
        assertEquals(StructMessageHandler.class, typeHandler.getClass());
    }

    @Test
    public void shouldReturnMessageProtoHandlerIfMessageFieldDescriptorPassed() {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        TypeHandler typeHandler = TypeHandlerFactory.getProtoHandler(messageFieldDescriptor);
        assertEquals(MessageHandler.class, typeHandler.getClass());
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
