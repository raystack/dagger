package com.gotocompany.dagger.common.serde.proto.deserialization;

import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.common.exceptions.DescriptorNotFoundException;
import com.gotocompany.dagger.common.exceptions.serde.DaggerDeserializationException;
import com.gotocompany.dagger.common.serde.typehandler.RowFactory;
import com.gotocompany.dagger.consumer.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.gotocompany.dagger.common.core.Constants.*;
import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoDeserializerTest {

    private StencilClientOrchestrator stencilClientOrchestrator;


    @Mock
    private Configuration configuration;

    @Mock
    private RowFactory rowFactory;

    @Before
    public void setUp() {
        initMocks(this);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }

    @Test
    public void shouldAlwaysReturnFalseForEndOfStream() {
        assertFalse(new ProtoDeserializer(TestBookingLogKey.class.getTypeName(), 4, "rowtime", stencilClientOrchestrator).isEndOfStream(null));
    }

    @Test
    public void shouldReturnProducedType() {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogKey.class.getTypeName(), 3, "rowtime", stencilClientOrchestrator);
        TypeInformation<Row> producedType = protoDeserializer.getProducedType();
        assertArrayEquals(
                new String[]{"service_type", "order_number", "order_url", "status", "event_timestamp", INTERNAL_VALIDATION_FIELD_KEY, "rowtime"},
                ((RowTypeInfo) producedType).getFieldNames());
        assertArrayEquals(
                new TypeInformation[]{STRING, STRING, STRING, STRING, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT), BOOLEAN, SQL_TIMESTAMP},
                ((RowTypeInfo) producedType).getFieldTypes());
    }

    @Test
    public void shouldDeserializeProtoAsRowWithSimpleFields() {
        String expectedOrderNumber = "111";
        final int expectedIterationNumber = 10;
        byte[] protoBytes = TestBookingLogMessage.newBuilder().setOrderNumber(expectedOrderNumber).setCancelReasonId(expectedIterationNumber).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        assertEquals(expectedOrderNumber, row.getField(bookingLogFieldIndex("order_number")));
        assertEquals(expectedIterationNumber, row.getField(bookingLogFieldIndex("cancel_reason_id")));
    }

    @Test
    public void shouldAddExtraFieldsToRow() {
        String expectedOrderNumber = "111";
        byte[] protoBytes = TestBookingLogMessage
                .newBuilder()
                .setOrderNumber(expectedOrderNumber)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1595548800L).setNanos(0).build())
                .build()
                .toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        int size = row.getArity();
        assertEquals(51, size);
        assertTrue("Didn't add field at the penultimate index", (Boolean) row.getField(size - 2));
        assertEquals(1595548800000L, ((java.sql.Timestamp) row.getField(size - 1)).getTime());
    }

    @Test
    public void shouldDeserializeEnumAsString() {

        byte[] protoBytes = TestBookingLogMessage.newBuilder().setServiceType(TestServiceType.Enum.GO_RIDE).setStatus(TestBookingStatus.Enum.COMPLETED).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        assertEquals(TestServiceType.Enum.GO_RIDE.toString(), row.getField(bookingLogFieldIndex("service_type")));
        assertEquals(TestBookingStatus.Enum.COMPLETED.toString(), row.getField(bookingLogFieldIndex("status")));
    }

    @Test
    public void shouldDeserializeNestedMessagesAsSubRows() {
        final int expectedSeconds = 10;
        final int expectedNanoSeconds = 10;
        final int expectedAccuracy = 111;
        final int expectedLatitude = 222;
        final int accuracyFieldIndex = 7;
        final int latitudeFieldIndex = 2;
        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(expectedSeconds).setNanos(expectedNanoSeconds).build();
        TestLocation testLocation = TestLocation.newBuilder().setAccuracyMeter(expectedAccuracy).setLatitude(expectedLatitude).build();
        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .setEventTimestamp(expectedTimestamp)
                .setDriverPickupLocation(testLocation).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Row eventTimestampRow = (Row) row.getField(bookingLogFieldIndex("event_timestamp"));
        assertEquals(expectedTimestamp.getSeconds(), eventTimestampRow.getField(0));
        assertEquals(expectedTimestamp.getNanos(), eventTimestampRow.getField(1));

        Row locationRow = (Row) row.getField(bookingLogFieldIndex("driver_pickup_location"));
        assertEquals(testLocation.getAccuracyMeter(), locationRow.getField(accuracyFieldIndex));
        assertEquals(testLocation.getLatitude(), locationRow.getField(latitudeFieldIndex));
    }

    @Test
    public void shouldDeserializeArrayOfObjectAsSubRows() throws IOException {
        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .setOrderNumber("EXAMPLE_ORDER_1")
                .addRoutes(TestRoute.newBuilder().setDistanceInKms(1.0f).setRouteOrder(4).build())
                .addRoutes(TestRoute.newBuilder().setDistanceInKms(2.0f).setRouteOrder(5).build())
                .addRoutes(TestRoute.newBuilder().setDistanceInKms(3.0f).setRouteOrder(6).build())
                .build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

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
        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .setOrderNumber("EXAMPLE-ID-01")
                .addMetaArray("EXAMPLE-REGISTERED-DEVICE-01")
                .addMetaArray("EXAMPLE-REGISTERED-DEVICE-02")
                .addMetaArray("EXAMPLE-REGISTERED-DEVICE-03")
                .addMetaArray("EXAMPLE-REGISTERED-DEVICE-04")
                .build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        String[] strings = (String[]) row.getField(bookingLogFieldIndex("meta_array"));

        assertEquals("EXAMPLE-REGISTERED-DEVICE-01", strings[0]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-02", strings[1]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-03", strings[2]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-04", strings[3]);

    }

    @Test
    public void shouldDeserializeProtobufMapAsSubRows() throws IOException {
        String orderNumber = "1";
        Map<String, String> currentState = new HashMap();
        currentState.put("force_close", "true");
        currentState.put("image", "example.png");

        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .putAllMetadata(currentState)
                .setOrderNumber(orderNumber).build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Object[] currentStateRowList = (Object[]) row.getField(bookingLogFieldIndex("metadata"));

        assertTrue(currentState.keySet().contains(((Row) currentStateRowList[0]).getField(0)));
        assertTrue(currentState.values().contains(((Row) currentStateRowList[0]).getField(1)));
        assertTrue(currentState.keySet().contains(((Row) currentStateRowList[1]).getField(0)));
        assertTrue(currentState.values().contains(((Row) currentStateRowList[1]).getField(1)));

        assertEquals(orderNumber, row.getField(bookingLogFieldIndex("order_number")));
    }

    @Test
    public void shouldDeserializeProtobufMapOfNullValueAsSubRows() throws IOException {
        String orderNumber = "1";
        Map<String, String> metaData = new HashMap<String, String>();
        metaData.put("force_close", "true");
        metaData.put("image", "");

        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .putAllMetadata(metaData)
                .setOrderNumber(orderNumber).build().toByteArray();

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Object[] currentStateRowList = (Object[]) row.getField(bookingLogFieldIndex("metadata"));

        assertTrue(metaData.keySet().contains(((Row) currentStateRowList[0]).getField(0)));
        assertTrue(metaData.values().contains(((Row) currentStateRowList[0]).getField(1)));
        assertTrue(metaData.keySet().contains(((Row) currentStateRowList[1]).getField(0)));
        assertTrue(metaData.values().contains(((Row) currentStateRowList[1]).getField(1)));

        assertEquals(orderNumber, row.getField(bookingLogFieldIndex("order_number")));
    }

    @Test
    public void shouldIgnoreStructWhileDeserialising() {
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

    @Test
    public void shouldThrowExceptionIfNotAbleToDeserialise() {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestNestedRepeatedMessage.class.getTypeName(), 6, "rowtime", stencilClientOrchestrator);
        assertThrows(DaggerDeserializationException.class,
                () -> protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, null)));
    }

    @Test
    public void shouldReturnInvalidRow() {
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);
        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, "test".getBytes()));
        assertFalse((boolean) row.getField(row.getArity() - 2));
        assertEquals(new java.sql.Timestamp(0), row.getField(row.getArity() - 1));
    }

    @Test
    public void shouldDeserializeProtoAsRowWithSimpleFieldsWhenStencilAutoRefreshEnabled() {
        String expectedOrderNumber = "111";
        final int expectedIterationNumber = 10;
        byte[] protoBytes = TestBookingLogMessage.newBuilder().setOrderNumber(expectedOrderNumber).setCancelReasonId(expectedIterationNumber).build().toByteArray();
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        assertEquals(expectedOrderNumber, row.getField(bookingLogFieldIndex("order_number")));
        assertEquals(expectedIterationNumber, row.getField(bookingLogFieldIndex("cancel_reason_id")));
    }

    @Test
    public void shouldAddExtraFieldsToRowWhenStencilAutoRefreshEnabled() {
        String expectedOrderNumber = "111";
        byte[] protoBytes = TestBookingLogMessage
                .newBuilder()
                .setOrderNumber(expectedOrderNumber)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1595548800L).setNanos(0).build())
                .build()
                .toByteArray();
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        int size = row.getArity();
        assertEquals(51, size);
        assertTrue("Didn't add field at the penultimate index", (Boolean) row.getField(size - 2));
        assertEquals(1595548800000L, ((java.sql.Timestamp) row.getField(size - 1)).getTime());
    }

    @Test
    public void shouldDeserializeEnumAsStringWhenStencilAutoRefreshEnabled() {

        byte[] protoBytes = TestBookingLogMessage.newBuilder().setServiceType(TestServiceType.Enum.GO_RIDE).setStatus(TestBookingStatus.Enum.COMPLETED).build().toByteArray();
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        assertEquals(TestServiceType.Enum.GO_RIDE.toString(), row.getField(bookingLogFieldIndex("service_type")));
        assertEquals(TestBookingStatus.Enum.COMPLETED.toString(), row.getField(bookingLogFieldIndex("status")));
    }

    @Test
    public void shouldDeserializeNestedMessagesAsSubRowsWhenStencilAutoRefreshEnabled() {
        final int expectedSeconds = 10;
        final int expectedNanoSeconds = 10;
        final int expectedAccuracy = 111;
        final int expectedLatitude = 222;
        final int accuracyFieldIndex = 7;
        final int latitudeFieldIndex = 2;
        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(expectedSeconds).setNanos(expectedNanoSeconds).build();
        TestLocation testLocation = TestLocation.newBuilder().setAccuracyMeter(expectedAccuracy).setLatitude(expectedLatitude).build();
        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .setEventTimestamp(expectedTimestamp)
                .setDriverPickupLocation(testLocation).build().toByteArray();

        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Row eventTimestampRow = (Row) row.getField(bookingLogFieldIndex("event_timestamp"));
        assertEquals(expectedTimestamp.getSeconds(), eventTimestampRow.getField(0));
        assertEquals(expectedTimestamp.getNanos(), eventTimestampRow.getField(1));

        Row locationRow = (Row) row.getField(bookingLogFieldIndex("driver_pickup_location"));
        assertEquals(testLocation.getAccuracyMeter(), locationRow.getField(accuracyFieldIndex));
        assertEquals(testLocation.getLatitude(), locationRow.getField(latitudeFieldIndex));
    }

    @Test
    public void shouldDeserializeArrayOfObjectAsSubRowsWhenStencilAutoRefreshEnabled() throws IOException {
        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .setOrderNumber("EXAMPLE_ORDER_1")
                .addRoutes(TestRoute.newBuilder().setDistanceInKms(1.0f).setRouteOrder(4).build())
                .addRoutes(TestRoute.newBuilder().setDistanceInKms(2.0f).setRouteOrder(5).build())
                .addRoutes(TestRoute.newBuilder().setDistanceInKms(3.0f).setRouteOrder(6).build())
                .build().toByteArray();
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

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
    public void shouldDeserializeArrayOfStringWhenStencilAutoRefreshEnabled() throws IOException {
        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .setOrderNumber("EXAMPLE-ID-01")
                .addMetaArray("EXAMPLE-REGISTERED-DEVICE-01")
                .addMetaArray("EXAMPLE-REGISTERED-DEVICE-02")
                .addMetaArray("EXAMPLE-REGISTERED-DEVICE-03")
                .addMetaArray("EXAMPLE-REGISTERED-DEVICE-04")
                .build().toByteArray();

        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));
        String[] strings = (String[]) row.getField(bookingLogFieldIndex("meta_array"));

        assertEquals("EXAMPLE-REGISTERED-DEVICE-01", strings[0]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-02", strings[1]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-03", strings[2]);
        assertEquals("EXAMPLE-REGISTERED-DEVICE-04", strings[3]);

    }

    @Test
    public void shouldDeserializeProtobufMapAsSubRowsWhenStencilAutoRefreshEnabled() throws IOException {
        String orderNumber = "1";
        Map<String, String> currentState = new HashMap();
        currentState.put("force_close", "true");
        currentState.put("image", "example.png");

        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .putAllMetadata(currentState)
                .setOrderNumber(orderNumber).build().toByteArray();

        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Object[] currentStateRowList = (Object[]) row.getField(bookingLogFieldIndex("metadata"));

        assertTrue(currentState.keySet().contains(((Row) currentStateRowList[0]).getField(0)));
        assertTrue(currentState.values().contains(((Row) currentStateRowList[0]).getField(1)));
        assertTrue(currentState.keySet().contains(((Row) currentStateRowList[1]).getField(0)));
        assertTrue(currentState.values().contains(((Row) currentStateRowList[1]).getField(1)));

        assertEquals(orderNumber, row.getField(bookingLogFieldIndex("order_number")));
    }

    @Test
    public void shouldDeserializeProtobufMapOfNullValueAsSubRowsWhenStencilAutoRefreshEnabled() throws IOException {
        String orderNumber = "1";
        Map<String, String> metaData = new HashMap<>();
        metaData.put("force_close", "true");
        metaData.put("image", "");

        byte[] protoBytes = TestBookingLogMessage.newBuilder()
                .putAllMetadata(metaData)
                .setOrderNumber(orderNumber).build().toByteArray();

        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);

        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));

        Object[] currentStateRowList = (Object[]) row.getField(bookingLogFieldIndex("metadata"));

        assertTrue(metaData.keySet().contains(((Row) currentStateRowList[0]).getField(0)));
        assertTrue(metaData.values().contains(((Row) currentStateRowList[0]).getField(1)));
        assertTrue(metaData.keySet().contains(((Row) currentStateRowList[1]).getField(0)));
        assertTrue(metaData.values().contains(((Row) currentStateRowList[1]).getField(1)));

        assertEquals(orderNumber, row.getField(bookingLogFieldIndex("order_number")));
    }

    @Test
    public void shouldIgnoreStructWhileDeserialisingWhenStencilAutoRefreshEnabled() {
        byte[] protoBytes = TestNestedRepeatedMessage.newBuilder()
                .addMetadata(Struct.getDefaultInstance())
                .addMetadata(Struct.getDefaultInstance())
                .setNumberField(5)
                .build().toByteArray();
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestNestedRepeatedMessage.class.getTypeName(), 6, "rowtime", stencilClientOrchestrator);
        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, protoBytes));
        assertNull(row.getField(4));
        assertEquals(row.getField(2), 5);
    }

    @Test
    public void shouldReturnInvalidRowWhenStencilAutoRefreshEnabled() {
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(true);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        ProtoDeserializer protoDeserializer = new ProtoDeserializer(TestBookingLogMessage.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);
        Row row = protoDeserializer.deserialize(new ConsumerRecord<>("test-topic", 0, 0, null, "test".getBytes()));
        assertFalse((boolean) row.getField(row.getArity() - 2));
        assertEquals(new java.sql.Timestamp(0), row.getField(row.getArity() - 1));
    }


    @Test
    public void shouldThrowDescriptorNotFoundExceptionForStringClass() {
        assertThrows(DescriptorNotFoundException.class,
                () -> new ProtoDeserializer(String.class.getTypeName(), 6, "rowtime", stencilClientOrchestrator));
    }

    private int bookingLogFieldIndex(String propertyName) {
        return TestBookingLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

    private int routeFieldIndex(String propertyName) {
        return TestRoute.getDescriptor().findFieldByName(propertyName).getIndex();
    }


}
