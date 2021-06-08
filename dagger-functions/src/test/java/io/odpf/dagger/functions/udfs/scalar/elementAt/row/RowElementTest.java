package io.odpf.dagger.functions.udfs.scalar.elementAt.row;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.functions.udfs.scalar.elementAt.descriptor.CustomDescriptor;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class RowElementTest {
    @Test
    public void shouldCreateNextAsRowElementForMessageType() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RowElement element = (RowElement) Element.initialize(null, null, new CustomDescriptor(getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage")), "routes").get();

        Optional<Element> next = element.createNext("start");

        assertEquals(RowElement.class, next.get().getClass());
    }

    @Test
    public void shouldCreateNextAsValueElementForNonMessageType() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RowElement element = (RowElement) Element.initialize(null, null, new CustomDescriptor(getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage")), "routes").get();

        Optional<Element> next = element.createNext("distance_in_kms");

        assertEquals(ValueElement.class, next.get().getClass());
    }

    @Test
    public void shouldHandleForInvalidPath() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RowElement element = (RowElement) Element.initialize(null, null, new CustomDescriptor(getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage")), "routes").get();

        Optional<Element> next = element.createNext("path");

        assertEquals(Optional.empty(), next);
    }

    @Test
    public void shouldInvokeParentOfElementMockWhenFetching() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RowElement rowElementMock = Mockito.mock(RowElement.class);
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);
        Mockito.when(rowElementMock.fetch()).thenReturn(routeRow);
        Descriptors.Descriptor bookingDescriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        Descriptors.Descriptor routeDescriptor = bookingDescriptor.getFields().get(43).getMessageType();
        Element childElement = Element.initialize(rowElementMock, null, new CustomDescriptor(routeDescriptor), "start").get();

        childElement.fetch();

        Mockito.verify(rowElementMock).fetch();
    }

    @Test
    public void shouldInvokeParentOfValueMockWhenFetching() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RowElement rowElementMock = Mockito.mock(RowElement.class);
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);
        Mockito.when(rowElementMock.fetch()).thenReturn(routeRow);
        Descriptors.Descriptor bookingDescriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        Descriptors.Descriptor routeDescriptor = bookingDescriptor.getFields().get(43).getMessageType();
        Element childElement = Element.initialize(rowElementMock, null, new CustomDescriptor(routeDescriptor), "distance_in_kms").get();

        childElement.fetch();

        Mockito.verify(rowElementMock).fetch();
    }

    @Test
    public void shouldNotInvokeParentOfElementMockWhenFetchingForRoot() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);
        Descriptors.Descriptor bookingDescriptor = getDescriptor("io.odpf.dagger.consumer.TestBookingLogMessage");
        Descriptors.Descriptor routeDescriptor = bookingDescriptor.getFields().get(43).getMessageType();
        Element rootElement = Element.initialize(null, routeRow, new CustomDescriptor(routeDescriptor), "distance_in_kms").get();

        rootElement.fetch();

        assertEquals(21.5F, rootElement.fetch());
    }

    private Descriptors.Descriptor getDescriptor(String protoClassName) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> protoClass = Class.forName(protoClassName);
        return (Descriptors.Descriptor) protoClass.getMethod("getDescriptor").invoke(null);
    }
}
