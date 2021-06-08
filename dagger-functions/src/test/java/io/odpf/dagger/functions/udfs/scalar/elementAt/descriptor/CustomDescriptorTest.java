package io.odpf.dagger.functions.udfs.scalar.elementAt.descriptor;

import com.google.protobuf.Descriptors;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class CustomDescriptorTest {
    @Test
    public void shouldGetTheFieldDescriptorForMessage() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(43);

        assertEquals(Optional.of(fieldDescriptor), customDescriptor.getFieldDescriptor("routes"));
    }

    @Test
    public void shouldGetTheFieldDescriptorIfNotMessage() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(0);

        assertEquals(Optional.of(fieldDescriptor), customDescriptor.getFieldDescriptor("service_type"));
    }

    @Test
    public void shouldHandleForFieldDescriptorIfNotValidPath() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        assertEquals(Optional.empty(), customDescriptor.getFieldDescriptor("invalid"));
    }

    @Test
    public void shouldGetTheDescriptorForMessage() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(43);

        assertEquals(Optional.of(fieldDescriptor.getMessageType()), customDescriptor.getDescriptor("routes"));
    }

    @Test
    public void shouldGetTheEmptyDescriptorIfNotAMessage() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        assertEquals(Optional.empty(), customDescriptor.getDescriptor("service_type"));
    }

    @Test
    public void shouldHandleForDescriptorIfNotValidPath() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        assertEquals(Optional.empty(), customDescriptor.getDescriptor("invalid"));
    }

    @Test
    public void shouldGetTheCustomDescriptorForMessage() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(43);

        assertEquals(Optional.of(new CustomDescriptor(fieldDescriptor.getMessageType())), customDescriptor.get("routes"));
    }

    @Test
    public void shouldGetTheEmptyCustomDescriptorIfNotAMessage() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        assertEquals(Optional.empty(), customDescriptor.get("service_type"));
    }

    @Test
    public void shouldHandleForCustomDescriptorIfNotValidPath() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Descriptors.Descriptor descriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        CustomDescriptor customDescriptor = new CustomDescriptor(descriptor);

        assertEquals(Optional.empty(), customDescriptor.get("invalid"));
    }

    @Test
    public void shouldHandleNullDescriptor() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        CustomDescriptor customDescriptor = new CustomDescriptor(null);

        assertEquals(Optional.empty(), customDescriptor.getDescriptor("invalid"));
    }

    private Descriptors.Descriptor getDescriptor(String protoClassName) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> protoClass = Class.forName(protoClassName);
        return (Descriptors.Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
    }
}
