package io.odpf.dagger.functions.udfs.scalar.elementAt.row;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.udfs.scalar.elementAt.descriptor.CustomDescriptor;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class ElementTest {
    @Test
    public void shouldInitializeValueElementWhenNotMessageType() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Optional<Element> element = Element.initialize(null, null, new CustomDescriptor(getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage")), "service_type");

        assertEquals(ValueElement.class, element.get().getClass());
    }

    @Test
    public void shouldInitializeRowElementWhenNotMessageType() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Optional<Element> element = Element.initialize(null, null, new CustomDescriptor(getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage")), "routes");

        assertEquals(RowElement.class, element.get().getClass());
    }

    @Test
    public void shouldHandleInvalidType() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Optional<Element> element = Element.initialize(null, null, new CustomDescriptor(getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage")), "invalid");

        assertEquals(Optional.empty(), element);
    }

    private Descriptors.Descriptor getDescriptor(String protoClassName) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> protoClass = Class.forName(protoClassName);
        return (Descriptors.Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
    }
}
