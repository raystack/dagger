package com.gojek.daggers.source;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DaggerDeserializationException;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.esb.booking.BookingLogKey;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.consumer.TestNestedRepeatedMessage;
import com.gojek.esb.gofood.AuditEntityLogMessage;
import com.gojek.esb.login.LoginRequestMessage;
import com.gojek.esb.logs.ApiLogMessage;
import com.gojek.esb.participant.DriverLocation;
import com.gojek.esb.participant.ParticipantLogMessage;
import com.gojek.esb.types.RouteProto;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;

import static com.gojek.daggers.utils.Constants.*;
import static com.gojek.esb.types.ParticipantStatusProto.ParticipantStatus.Enum.ACCEPTED;
import static com.gojek.esb.types.ServiceTypeProto.ServiceType.Enum.GO_AUTO;
import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class ProtoDeserializerTest {

    private Configuration configuration;

    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setUp() {
        initMocks(this);
        configuration = new Configuration();
        configuration.setString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT);
        configuration.setString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT);
        configuration.setBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT);
        configuration.setString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }

    @Test
    public void shouldAlwaysReturnFalseForEndOfStream() {
        assertFalse(new ProtoDeserializer(BookingLogKey.class.getTypeName(), 4, "rowtime", stencilClientOrchestrator).isEndOfStream(null));
    }

    @Test
    public void shouldDeserializeProtoAsRowWithSimpleFields() throws IOException {

        String expectedOrderNumber = "111";
        final int expectedIterationNumber = 10;
        byte[] protoBytes = ParticipantLogMessage.newBuilder().setOrderId(expectedOrderNumber)
                .setIterationNumber(expectedIterationNumber).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), 3, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        assertEquals(expectedOrderNumber, row.getField(participantLogFieldIndex("order_id")));
        assertEquals(expectedIterationNumber, row.getField(participantLogFieldIndex("iteration_number")));
    }

    @Test
    public void shouldDeserializeEnumAsString() throws IOException {

        byte[] protoBytes = ParticipantLogMessage.newBuilder().setServiceType(GO_AUTO).setStatus(ACCEPTED).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), 3, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        assertEquals(GO_AUTO.toString(), row.getField(participantLogFieldIndex("service_type")));
        assertEquals(ACCEPTED.toString(), row.getField(participantLogFieldIndex("status")));
    }

    @Test
    public void shouldDeserializeNestedMessagesAsSubRows() throws IOException {
        final int expectedSeconds = 10;
        final int expectedNanoSeconds = 10;
        final int expectedAccuracy = 111;
        final int expectedLatitude = 222;
        final int accuracyFieldIndex = 3;
        final int latitudeFieldIndex = 0;
        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(expectedSeconds).setNanos(expectedNanoSeconds).build();
        DriverLocation expectedDriverLocation = DriverLocation.newBuilder().setAccuracy(expectedAccuracy).setLatitude(expectedLatitude).build();
        byte[] protoBytes = ParticipantLogMessage.newBuilder()
                .setEventTimestamp(expectedTimestamp)
                .setLocation(expectedDriverLocation).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), 3, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Row eventTimestampRow = (Row) row.getField(participantLogFieldIndex("event_timestamp"));
        assertEquals(expectedTimestamp.getSeconds(), eventTimestampRow.getField(0));
        assertEquals(expectedTimestamp.getNanos(), eventTimestampRow.getField(1));

        Row locationRow = (Row) row.getField(participantLogFieldIndex("location"));
        assertEquals(expectedDriverLocation.getAccuracy(), locationRow.getField(accuracyFieldIndex));
        assertEquals(expectedDriverLocation.getLatitude(), locationRow.getField(latitudeFieldIndex));
    }

    @Test
    public void shouldDeserializeArrayOfObjectAsSubRows() throws IOException {
        byte[] protoBytes = BookingLogMessage.newBuilder()
                .setOrderNumber("EXAMPLE_ORDER_1")
                .addRoutes(RouteProto.Route.newBuilder().setDistanceInKms(1.0f).setRouteOrder(4).build())
                .addRoutes(RouteProto.Route.newBuilder().setDistanceInKms(2.0f).setRouteOrder(5).build())
                .addRoutes(RouteProto.Route.newBuilder().setDistanceInKms(3.0f).setRouteOrder(6).build())
                .build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(BookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Object[] routes = (Object[]) row.getField(bookingLogFieldIndex("routes"));
        Row firstRouteRow = (Row) routes[0];
        assertEquals(firstRouteRow.getField(routeFieldIndex("distance_in_kms")), 1.0f);
        assertEquals(firstRouteRow.getField(routeFieldIndex("route_order")), 4);

        Row secondRouteRow = (Row) routes[1];
        assertEquals(secondRouteRow.getField(routeFieldIndex("distance_in_kms")), 2.0f);
        assertEquals(secondRouteRow.getField(routeFieldIndex("route_order")), 5);

        Row thirdRouteRow = (Row) routes[2];
        assertEquals(thirdRouteRow.getField(routeFieldIndex("distance_in_kms")), 3.0f);
        assertEquals(thirdRouteRow.getField(routeFieldIndex("route_order")), 6);
    }

    @Test
    public void shouldDeserializeArrayOfString() throws IOException {
        byte[] protoBytes = LoginRequestMessage.newBuilder()
                .setRequestId("EXAMPLE-ID-01")
                .addRegisteredDevice("EXAMPLE-REGISTERED-DEVICE-01")
                .addRegisteredDevice("EXAMPLE-REGISTERED-DEVICE-02")
                .addRegisteredDevice("EXAMPLE-REGISTERED-DEVICE-03")
                .addRegisteredDevice("EXAMPLE-REGISTERED-DEVICE-04")
                .build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(LoginRequestMessage.class.getTypeName(), 9, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        String[] registeredDevices = (String[]) row.getField(loginRequestFieldIndex("registered_device"));

        assertEquals("EXAMPLE-REGISTERED-DEVICE-01", registeredDevices[0]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-02", registeredDevices[1]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-03", registeredDevices[2]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-04", registeredDevices[3]);

    }

    @Test
    public void shouldDeserializeArrayOfStringInApiLog() throws IOException {
        byte[] protoBytes = ApiLogMessage.newBuilder()
                .addRequestHeadersExtra("EXAMPLE-REGISTERED-DEVICE-01")
                .addRequestHeadersExtra("EXAMPLE-REGISTERED-DEVICE-02")
                .addRequestHeadersExtra("EXAMPLE-REGISTERED-DEVICE-03")
                .addRequestHeadersExtra("EXAMPLE-REGISTERED-DEVICE-04")
                .build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ApiLogMessage.class.getTypeName(), 1, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        String[] registeredDevices = (String[]) row.getField(apiLogMessageFieldIndex("request_headers_extra"));

        assertEquals("EXAMPLE-REGISTERED-DEVICE-01", registeredDevices[0]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-02", registeredDevices[1]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-03", registeredDevices[2]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-04", registeredDevices[3]);

    }

    @Test
    public void shouldDeserializeProtobufMapAsSubRows() throws IOException {
        String auditId = "1";
        HashMap currentState = new HashMap<String, String>();
        currentState.put("force_close", "true");
        currentState.put("image", "example.png");

        byte[] protoBytes = AuditEntityLogMessage.newBuilder()
                .putAllCurrentState(currentState)
                .setAuditId(auditId).build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(AuditEntityLogMessage.class.getTypeName(), 4, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Object[] currentStateRowList = (Object[]) row.getField(auditEntityLogFieldIndex("current_state"));

        assertTrue(currentState.keySet().contains(((Row) currentStateRowList[0]).getField(0)));
        assertTrue(currentState.values().contains(((Row) currentStateRowList[0]).getField(1)));
        assertTrue(currentState.keySet().contains(((Row) currentStateRowList[1]).getField(0)));
        assertTrue(currentState.values().contains(((Row) currentStateRowList[1]).getField(1)));

        assertEquals(auditId, row.getField(auditEntityLogFieldIndex("audit_id")));
    }

    @Test
    public void shouldDeserializeProtobufMapOfNullValueAsSubRows() throws IOException {
        String auditId = "1";
        HashMap currentState = new HashMap<String, String>();
        currentState.put("force_close", "true");
        currentState.put("image", "");

        byte[] protoBytes = AuditEntityLogMessage.newBuilder()
                .putAllCurrentState(currentState)
                .setAuditId(auditId).build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(AuditEntityLogMessage.class.getTypeName(), 4, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Object[] currentStateRowList = (Object[]) row.getField(auditEntityLogFieldIndex("current_state"));

        assertTrue(currentState.keySet().contains(((Row) currentStateRowList[0]).getField(0)));
        assertTrue(currentState.values().contains(((Row) currentStateRowList[0]).getField(1)));
        assertTrue(currentState.keySet().contains(((Row) currentStateRowList[1]).getField(0)));
        assertTrue(currentState.values().contains(((Row) currentStateRowList[1]).getField(1)));

        assertEquals(auditId, row.getField(auditEntityLogFieldIndex("audit_id")));
    }

    @Test
    public void shouldIgnoreStructWhileDeserialising() throws IOException {
        byte[] protoBytes = TestNestedRepeatedMessage.newBuilder()
                .addMetadata(Struct.getDefaultInstance())
                .addMetadata(Struct.getDefaultInstance())
                .setNumberField(5)
                .build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestNestedRepeatedMessage.class.getTypeName(), 6, "rowtime", stencilClientOrchestrator);
        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));
        assertNull(row.getField(4));
        assertEquals(row.getField(2), 5);
    }

    @Test(expected = DaggerDeserializationException.class)
    public void shouldThrowExceptionIfNotAbleToDeserialise() throws IOException {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestNestedRepeatedMessage.class.getTypeName(), 6, "rowtime", stencilClientOrchestrator);
        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, null));
    }

    @Test(expected = DescriptorNotFoundException.class)
    public void shouldThrowDescriptorNotFoundException() throws IOException {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer("randomProtoClass", 6, "rowtime", stencilClientOrchestrator);
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void shouldThrowInvalidProtocolBufferException() throws IOException {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(BookingLogMessage.class.getTypeName(), 6, "rowtime", stencilClientOrchestrator);
        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, "test".getBytes()));
    }

    private int participantLogFieldIndex(String propertyName) {
        return ParticipantLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

    private int auditEntityLogFieldIndex(String propertyName) {
        return AuditEntityLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

    private int bookingLogFieldIndex(String propertyName) {
        return BookingLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

    private int routeFieldIndex(String propertyName) {
        return RouteProto.Route.getDescriptor().findFieldByName(propertyName).getIndex();
    }

    private int loginRequestFieldIndex(String propertyName) {
        return LoginRequestMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

    private int apiLogMessageFieldIndex(String propertyName) {
        return ApiLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }
}
