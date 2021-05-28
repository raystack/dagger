package io.odpf.dagger.functions.udfs.scalar.elementAt.row;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.udfs.scalar.elementAt.descriptor.CustomDescriptor;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class ValueElementTest {
    @Test
    public void shouldCreateEmptyNextElementForInvalidValueObject() {
        ValueElement valueElement = new ValueElement(null, null, null);

        assertEquals(Optional.empty(), valueElement.createNext(""));
    }

    @Test
    public void shouldCreateNextAsEmptyForValidValueObject() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        ValueElement element = (ValueElement) Element.initialize(null, null, new CustomDescriptor(getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage")), "service_type").get();

        Optional<Element> next = element.createNext("valid_path_not_possible");

        assertEquals(Optional.empty(), next);
    }

    private Descriptors.Descriptor getDescriptor(String protoClassName) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> protoClass = Class.forName(protoClassName);
        return (Descriptors.Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
    }
}
