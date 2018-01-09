package com.gojek.daggers;

import com.gojek.esb.booking.BookingLogKey;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.gofood.AuditEntityLogMessage;
import com.gojek.esb.login.LoginRequestMessage;
import com.gojek.esb.logs.ApiLogMessage;
import com.gojek.esb.participant.DriverLocation;
import com.gojek.esb.participant.ParticipantLogMessage;
import com.gojek.esb.types.RouteProto;
import com.google.protobuf.Timestamp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;

import static com.gojek.esb.types.ParticipantStatusProto.ParticipantStatus.Enum.ACCEPTED;
import static com.gojek.esb.types.ServiceTypeProto.ServiceType.Enum.GO_AUTO;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class ProtoDeserializerTest {

  @Mock
  private ProtoType protoType;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void shouldGetTypeInfomationFromProtoType() {

    String[] expectedFieldNames = {"field1", "field2"};
    TypeInformation<?>[] expectedTypes = {Types.DOUBLE, Types.STRING};
    RowTypeInfo expectedRowType = new RowTypeInfo();
    when(protoType.getRowType()).thenReturn(expectedRowType);

    TypeInformation<Row> actualType = new ProtoDeserializer(BookingLogKey.class.getTypeName(), protoType).getProducedType();
    assertEquals(expectedRowType, actualType);
  }

  @Test
  public void shouldAlwaysReturnFalseForEndOfStream() {
    assertFalse(new ProtoDeserializer(BookingLogKey.class.getTypeName(), protoType).isEndOfStream(null));
  }

  @Test
  public void shouldDeserializeProtoAsRowWithSimpelFields() throws IOException {

    String expectedOrderNumber = "111";
    final int expectedIterationNumber = 10;
    byte[] protoBytes = ParticipantLogMessage.newBuilder().setOrderId(expectedOrderNumber)
        .setIterationNumber(expectedIterationNumber).build().toByteArray();
    ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), protoType);

    Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

    assertEquals(expectedOrderNumber, row.getField(participantLogFieldIndex("order_id")));
    assertEquals(expectedIterationNumber, row.getField(participantLogFieldIndex("iteration_number")));
  }

  @Test
  public void shouldDeserializeEnumAsString() throws IOException {

    byte[] protoBytes = ParticipantLogMessage.newBuilder().setServiceType(GO_AUTO).setStatus(ACCEPTED).build().toByteArray();
    ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), protoType);

    Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

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
    ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), protoType);

    Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

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

    ProtoDeserializer protoDeserializer = new ProtoDeserializer(BookingLogMessage.class.getTypeName(), protoType);

    Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

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

    ProtoDeserializer protoDeserializer = new ProtoDeserializer(LoginRequestMessage.class.getTypeName(), protoType);

    Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

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

    ProtoDeserializer protoDeserializer = new ProtoDeserializer(ApiLogMessage.class.getTypeName(), protoType);

    Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

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

    ProtoDeserializer protoDeserializer = new ProtoDeserializer(AuditEntityLogMessage.class.getTypeName(), protoType);

    Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

    Object[] currentStateRowList = (Object[]) row.getField(auditEntityLogFieldIndex("current_state"));

    assertTrue(currentState.keySet().contains(((Row) currentStateRowList[0]).getField(0)));
    assertTrue(currentState.values().contains(((Row) currentStateRowList[0]).getField(1)));
    assertTrue(currentState.keySet().contains(((Row) currentStateRowList[1]).getField(0)));
    assertTrue(currentState.values().contains(((Row) currentStateRowList[1]).getField(1)));

    assertEquals(auditId, row.getField(auditEntityLogFieldIndex("audit_id")));
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
