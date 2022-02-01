package io.odpf.dagger.functions.udfs.scalar.elementAt;

import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MessageReaderTest {
    private static final double CENTRAL_MONUMENT_JAKARTA_LATITUDE = -6.170024;
    private static final double CENTRAL_MONUMENT_JAKARTA_LONGITUDE = 106.8243203;
    private static final StencilClient STENCIL_CLIENT = StencilClientFactory.getClient();

    @Test
    public void shouldBeAbleToReadPathOfOneDepth() throws ClassNotFoundException {
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);

        MessageReader messageReader = new MessageReader(routeRow, "io.odpf.dagger.consumer.TestBookingLogMessage", "routes", STENCIL_CLIENT);

        assertEquals(21.5F, messageReader.read("distance_in_kms"));
    }

    @Test
    public void shouldBeAbleToReadPathOfTwoDepth() throws ClassNotFoundException {
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        locationRow.setField(3, CENTRAL_MONUMENT_JAKARTA_LONGITUDE);
        routeRow.setField(0, locationRow);

        MessageReader messageReader = new MessageReader(routeRow, "io.odpf.dagger.consumer.TestBookingLogMessage", "routes", STENCIL_CLIENT);

        assertEquals(CENTRAL_MONUMENT_JAKARTA_LATITUDE, messageReader.read("start.latitude"));
        assertEquals(CENTRAL_MONUMENT_JAKARTA_LONGITUDE, messageReader.read("start.longitude"));
    }

    @Test
    public void shouldHandleWhenNoValidProtobufForMessageRoute() throws ClassNotFoundException {
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);

        MessageReader messageReader = new MessageReader(routeRow, "io.odpf.dagger.consumer.TestBookingLogMessage", "invalid", STENCIL_CLIENT);

        assertEquals("", messageReader.read("distance_in_kms"));
    }

    @Test(expected = ClassNotFoundException.class)
    public void shouldHandleWhenNoValidRootProtobuf() throws ClassNotFoundException {
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);

        MessageReader messageReader = new MessageReader(routeRow, "invalid", "routes", STENCIL_CLIENT);
        messageReader.read("anything");
    }

    @Test
    public void shouldHandleInvalidParentPathOfOneDepth() throws ClassNotFoundException {
        Row routeRow = new Row(3);
        routeRow.setField(2, 21.5F);

        MessageReader messageReader = new MessageReader(routeRow, "io.odpf.dagger.consumer.TestBookingLogMessage", "routes", STENCIL_CLIENT);

        assertEquals("", messageReader.read("invalid"));
    }

    @Test
    public void shouldHandleInvalidPathAtParentWhenReadingNestedField() throws ClassNotFoundException {
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        routeRow.setField(0, locationRow);

        MessageReader messageReader = new MessageReader(routeRow, "io.odpf.dagger.consumer.TestBookingLogMessage", "routes", STENCIL_CLIENT);

        assertEquals("", messageReader.read("invalid.start"));
    }

    @Test
    public void shouldHandleInvalidNestedPath() throws ClassNotFoundException {
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        routeRow.setField(0, locationRow);

        MessageReader messageReader = new MessageReader(routeRow, "io.odpf.dagger.consumer.TestBookingLogMessage", "routes", STENCIL_CLIENT);

        assertEquals("", messageReader.read("start.invalid"));
    }

    @Test
    public void shouldBeAbleToReadObject() throws ClassNotFoundException {
        Row routeRow = new Row(3);
        Row locationRow = new Row(4);
        locationRow.setField(2, CENTRAL_MONUMENT_JAKARTA_LATITUDE);
        routeRow.setField(0, locationRow);

        MessageReader messageReader = new MessageReader(routeRow, "io.odpf.dagger.consumer.TestBookingLogMessage", "routes", STENCIL_CLIENT);

        assertEquals(locationRow, messageReader.read("start"));
    }
}
