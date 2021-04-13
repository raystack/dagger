package io.odpf.dagger.sink;

import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.exception.DaggerSerializationException;
import io.odpf.dagger.exception.DescriptorNotFoundException;
import io.odpf.dagger.exception.InvalidColumnMappingException;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.esb.aggregate.demand.AggregatedDemandKey;
import com.gojek.esb.aggregate.demand.AggregatedDemandMessage;
import com.gojek.esb.aggregate.supply.AggregatedSupplyKey;
import com.gojek.esb.aggregate.supply.AggregatedSupplyMessage;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.customer.CustomerLogMessage;
import com.gojek.esb.fraud.EnrichedBookingLogMessage;
import com.gojek.esb.types.BookingStatusProto;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.gojek.esb.types.ServiceTypeProto.ServiceType.Enum.GO_RIDE;
import static com.gojek.esb.types.VehicleTypeProto.VehicleType.Enum.BIKE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoSerializerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    private StencilClient stencilClient;
    private String outputTopic = "test-topic";

    @Before
    public void setup() {
        initMocks(this);
        stencilClient = StencilClientFactory.getClient();
        when(stencilClientOrchestrator.getStencilClient()).thenReturn(stencilClient);
    }

    @Test
    public void shouldSerializeKeyForDemandProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type"};
        String outputProtoKey = "com.gojek.esb.aggregate.demand.AggregatedDemandKey";
        String outputProtoMessage = "com.gojek.esb.aggregate.demand.AggregatedDemandMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        long seconds = System.currentTimeMillis() / 1000;

        Row element = new Row(5);
        Timestamp timestamp = new Timestamp(seconds * 1000);
        com.google.protobuf.Timestamp expectedTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(seconds)
                .setNanos(0)
                .build();

        element.setField(0, timestamp);
        element.setField(1, timestamp);
        element.setField(2, 13);
        element.setField(3, 3322909458387959808L);
        element.setField(4, GO_RIDE);

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        AggregatedDemandKey actualKey = AggregatedDemandKey.parseFrom(producerRecord.key());

        assertEquals(expectedTimestamp, actualKey.getWindowStartTime());
        assertEquals(expectedTimestamp, actualKey.getWindowEndTime());
        assertEquals(13, actualKey.getS2IdLevel());
        assertEquals(3322909458387959808L, actualKey.getS2Id());
        assertEquals("GO_RIDE", actualKey.getServiceType().toString());
    }

    @Test
    public void shouldSerializeValueForDemandProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type", "unique_customers"};
        String outputProtoKey = "com.gojek.esb.aggregate.demand.AggregatedDemandKey";
        String outputProtoMessage = "com.gojek.esb.aggregate.demand.AggregatedDemandMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        long seconds = System.currentTimeMillis() / 1000;

        Row element = new Row(6);
        Timestamp timestamp = new Timestamp(seconds * 1000);
        com.google.protobuf.Timestamp expectedTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(seconds)
                .setNanos(0)
                .build();

        element.setField(0, timestamp);
        element.setField(1, timestamp);
        element.setField(2, 13);
        element.setField(3, 3322909458387959808L);
        element.setField(4, GO_RIDE);
        element.setField(5, 2L);

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        AggregatedDemandMessage actualValue = AggregatedDemandMessage.parseFrom(producerRecord.value());

        assertEquals(expectedTimestamp, actualValue.getWindowStartTime());
        assertEquals(expectedTimestamp, actualValue.getWindowEndTime());
        assertEquals(13, actualValue.getS2IdLevel());
        assertEquals(3322909458387959808L, actualValue.getS2Id());
        assertEquals("GO_RIDE", actualValue.getServiceType().toString());
        assertEquals(2L, actualValue.getUniqueCustomers());
    }

    @Test
    public void shouldSerializeKeyForSupplyProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "vehicle_type"};
        String outputProtoKey = "com.gojek.esb.aggregate.supply.AggregatedSupplyKey";
        String outputProtoMessage = "com.gojek.esb.aggregate.supply.AggregatedSupplyMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        long startTimeInSeconds = System.currentTimeMillis() / 1000;
        long endTimeInSeconds = (System.currentTimeMillis() + 10000) / 1000;

        Row element = new Row(5);
        Timestamp startTimestamp = new java.sql.Timestamp(startTimeInSeconds * 1000);
        Timestamp endTimestamp = new java.sql.Timestamp(endTimeInSeconds * 1000);
        com.google.protobuf.Timestamp expectedStartTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(startTimeInSeconds)
                .setNanos(0)
                .build();
        com.google.protobuf.Timestamp expectedEndTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(endTimeInSeconds)
                .setNanos(0)
                .build();

        element.setField(0, startTimestamp);
        element.setField(1, endTimestamp);
        element.setField(2, 13);
        element.setField(3, 3322909458387959808L);
        element.setField(4, BIKE);

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);


        AggregatedSupplyKey actualKey = AggregatedSupplyKey.parseFrom(producerRecord.key());

        assertEquals(expectedStartTimestamp, actualKey.getWindowStartTime());
        assertEquals(expectedEndTimestamp, actualKey.getWindowEndTime());
        assertEquals(13, actualKey.getS2IdLevel());
        assertEquals(3322909458387959808L, actualKey.getS2Id());
        assertEquals("BIKE", actualKey.getVehicleType().toString());
    }

    @Test
    public void shouldSerializeValueForSupplyProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "vehicle_type", "unique_drivers"};
        String outputProtoKey = "com.gojek.esb.aggregate.supply.AggregatedSupplyKey";
        String outputProtoMessage = "com.gojek.esb.aggregate.supply.AggregatedSupplyMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        long startTimeInSeconds = System.currentTimeMillis() / 1000;
        long endTimeInSeconds = (System.currentTimeMillis() + 10000) / 1000;

        Row element = new Row(6);
        Timestamp startTimestamp = new java.sql.Timestamp(startTimeInSeconds * 1000);
        Timestamp endTimestamp = new java.sql.Timestamp(endTimeInSeconds * 1000);
        com.google.protobuf.Timestamp expectedStartTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(startTimeInSeconds)
                .setNanos(0)
                .build();
        com.google.protobuf.Timestamp expectedEndTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(endTimeInSeconds)
                .setNanos(0)
                .build();

        element.setField(0, startTimestamp);
        element.setField(1, endTimestamp);
        element.setField(2, 13);
        element.setField(3, 3322909458387959808L);
        element.setField(4, BIKE);
        element.setField(5, 2L);

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        AggregatedSupplyMessage actualValue = AggregatedSupplyMessage.parseFrom(producerRecord.value());

        assertEquals(expectedStartTimestamp, actualValue.getWindowStartTime());
        assertEquals(expectedEndTimestamp, actualValue.getWindowEndTime());
        assertEquals(13, actualValue.getS2IdLevel());
        assertEquals(3322909458387959808L, actualValue.getS2Id());
        assertEquals("BIKE", actualValue.getVehicleType().toString());
        assertEquals(2L, actualValue.getUniqueDrivers());
    }

    @Test
    public void shouldSerializeValueForEnrichedBookingLogProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"service_type", "order_url", "status", "order_number", "event_timestamp", "customer_id", "customer_url", "driver_id", "driver_url", "helooo"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        com.google.protobuf.Timestamp timestampProto = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(((System.currentTimeMillis() + 10000) / 1000))
                .setNanos(0)
                .build();

        long endTimeInSeconds = (System.currentTimeMillis() + 10000) / 1000;
        Timestamp timestamp = new java.sql.Timestamp(endTimeInSeconds * 1000);
        Row element = new Row(columnNames.length);
        element.setField(0, GO_RIDE);
        element.setField(1, "TEST");
        element.setField(2, BookingStatusProto.BookingStatus.Enum.COMPLETED);
        element.setField(3, "12345");
        element.setField(4, timestamp);
        element.setField(5, "67890");
        element.setField(6, "CUST_TEST");
        element.setField(7, "1234556677");
        element.setField(8, "DRIVER_TEST");
        element.setField(9, "world");

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(producerRecord.value());

        assertNotNull(bookingLogMessage);
        assertEquals(timestampProto, bookingLogMessage.getEventTimestamp());
        assertEquals("12345", bookingLogMessage.getOrderNumber());

        String jsonString = "{\n" +
                " \"_index\": \"customers\",\n" +
                " \"_type\": \"customer\",\n" +
                " \"_id\": \"547774090\",\n" +
                " \"_version\": 11,\n" +
                " \"found\": true,\n" +
                " \"_source\": {\n" +
                "   \"created_at\": \"2015-11-06T12:47:20Z\",\n" +
                "   \"customer_id\": \"547774090\",\n" +
                "   \"email\": \"asfarudin@go-jek.com\",\n" +
                "   \"is_fb_linked\": false,\n" +
                "   \"phone\": \"+6285725742946\",\n" +
                "   \"name\": \"Asfar Daiman\",\n" +
                "   \"wallet_id\": \"16230120432734932822\",\n" +
                "   \"type\": \"customer\",\n" +
                "   \"blacklisted\": null,\n" +
                "   \"updated_at\": 1537423759,\n" +
                "   \"active\": true,\n" +
                "   \"wallet-id\": \"16230120432734932822\",\n" +
                "   \"last_login_date\": 1549512271\n" +
                " }\n" +
                "}";

        String updatedJsonString = Arrays.stream(jsonString.split(",")).filter((val) -> !(val.contains("updated_at"))).collect(Collectors.joining(","));

        String customerSource = updatedJsonString.substring(updatedJsonString.indexOf("\"_source\": {") + 11, updatedJsonString.indexOf("}") + 1);

        CustomerLogMessage.Builder customerLogBuilder = CustomerLogMessage.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(customerSource, customerLogBuilder);
        CustomerLogMessage customerLogMessage = customerLogBuilder.build();
        assertNotNull(customerLogMessage);
        assertEquals("547774090", customerLogMessage.getCustomerId());

        EnrichedBookingLogMessage enrichedBookingLogMessage = EnrichedBookingLogMessage.newBuilder()
                .mergeBookingLog(bookingLogMessage)
                .mergeCustomerProfile(customerLogMessage)
                .build();
        assertNotNull(enrichedBookingLogMessage);
        assertEquals(timestampProto, enrichedBookingLogMessage.getBookingLog().getEventTimestamp());
        assertEquals("547774090", enrichedBookingLogMessage.getCustomerProfile().getCustomerId());
    }

    @Test
    public void shouldSerializeDataForOneFieldInNestedProtoWhenMappedFromQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"customer_profile.name"};
        String outputProtoKey = "com.gojek.esb.fraud.EnrichedBookingLogKey";
        String outputProtoMessage = "com.gojek.esb.fraud.EnrichedBookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);

        Row element = new Row(1);

        element.setField(0, "test-name");

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        EnrichedBookingLogMessage actualValue = EnrichedBookingLogMessage.parseFrom(producerRecord.value());

        assertEquals("test-name", actualValue.getCustomerProfile().getName());
    }

    @Test
    public void shouldSerializeDataForMultipleFieldsInSameNestedProtoWhenMappedFromQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"customer_profile.name", "customer_profile.email", "customer_profile.phone_verified"};
        String outputProtoKey = "com.gojek.esb.fraud.EnrichedBookingLogKey";
        String outputProtoMessage = "com.gojek.esb.fraud.EnrichedBookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);

        Row element = new Row(3);

        element.setField(0, "test-name");
        element.setField(1, "test_email@go-jek.com");
        element.setField(2, true);

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        EnrichedBookingLogMessage actualValue = EnrichedBookingLogMessage.parseFrom(producerRecord.value());

        assertEquals("test-name", actualValue.getCustomerProfile().getName());
        assertEquals("test_email@go-jek.com", actualValue.getCustomerProfile().getEmail());
        assertTrue(actualValue.getCustomerProfile().getPhoneVerified());
    }

    @Test
    public void shouldSerializeDataForMultipleFieldsInDifferentNestedProtoWhenMappedFromQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"order_number", "service_type", "customer_price", "customer_total_fare_without_surge", "driver_pickup_location.name", "driver_pickup_location.latitude"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);

        Row element = new Row(6);

        element.setField(0, "order_number");
        element.setField(1, "GO_RIDE");
        element.setField(2, 123F);
        element.setField(3, 12345L);
        element.setField(4, "driver_name");
        element.setField(5, 876D);

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        BookingLogMessage actualValue = BookingLogMessage.parseFrom(producerRecord.value());

        assertEquals("order_number", actualValue.getOrderNumber());
        assertEquals(GO_RIDE, actualValue.getServiceType());
        assertEquals(123F, actualValue.getCustomerPrice(), 0F);
        assertEquals(12345L, actualValue.getCustomerTotalFareWithoutSurge());
        assertEquals("driver_name", actualValue.getDriverPickupLocation().getName());
        assertEquals(876D, actualValue.getDriverPickupLocation().getLatitude(), 0D);
    }

    @Test
    public void shouldThrowExceptionWhenColumnDoesNotExists() {
        expectedException.expect(InvalidColumnMappingException.class);
        expectedException.expectMessage("column invalid doesn't exists in the proto of gojek.esb.types.Location");

        String[] columnNames = {"order_number", "driver_pickup_location.invalid"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        Row element = new Row(2);
        element.setField(0, "order_number");
        element.setField(1, 876D);

        protoSerializer.serialize(element, null);
    }

    @Test
    public void shouldMapOtherFieldsWhenOneOfTheFirstFieldIsInvalidForANestedFieldInTheQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"blah.invalid", "customer_email"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        Row element = new Row(2);
        element.setField(0, "order_number");
        element.setField(1, "customer_email@go-jek.com");

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        assertEquals("customer_email@go-jek.com", BookingLogMessage.parseFrom(producerRecord.value()).getCustomerEmail());
    }

    @Test
    public void shouldMapEmptyDataWhenFieldIsInvalidInTheQuery() {
        String[] columnNames = {"invalid"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        Row element = new Row(1);
        element.setField(0, "order_number");

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        assertEquals(0, producerRecord.value().length);
    }

    @Test
    public void shouldMapOtherFieldsWhenOneOfTheFieldIsInvalidInTheQuery() throws InvalidProtocolBufferException {
        String[] columnNames = {"invalid", "order_number"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        Row element = new Row(2);
        element.setField(0, "some_data");
        element.setField(1, "order_number");

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        assertEquals("order_number", BookingLogMessage.parseFrom(producerRecord.value()).getOrderNumber());
    }

    @Test
    public void shouldThrowExceptionWhenTypeDoesNotMatch() {
        expectedException.expect(InvalidColumnMappingException.class);
        expectedException.expectMessage("column invalid: type mismatch of column order_number, expecting STRING type. Actual type class java.lang.Integer");

        String[] columnNames = {"order_number"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        Row element = new Row(1);
        element.setField(0, 1234);

        protoSerializer.serialize(element, null);
    }

    @Test
    public void shouldHandleRepeatedTypeWhenTypeDoesNotMatch() {
        expectedException.expect(InvalidColumnMappingException.class);
        expectedException.expectMessage("column invalid: type mismatch of column members, expecting REPEATED STRING type. Actual type class java.lang.Integer");

        String[] columnNames = {"members"};
        String outputProtoKey = "com.gojek.esb.segmentation.UpdateLogKey";
        String outputProtoMessage = "com.gojek.esb.segmentation.UpdateLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        Row element = new Row(1);
        element.setField(0, 1234);

        protoSerializer.serialize(element, null);
    }

    @Test
    public void shouldSerializeMessageWhenOnlyMessageProtoProvided() throws InvalidProtocolBufferException {
        String[] columnNames = {"order_number", "driver_id"};
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(null, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);

        String orderNumber = "RB-1234";

        Row element = new Row(2);
        element.setField(0, orderNumber);
        element.setField(1, "DR-124");

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);
        BookingLogMessage actualMessage = BookingLogMessage.parseFrom(producerRecord.value());

        assertEquals(orderNumber, actualMessage.getOrderNumber());
    }

    @Test
    public void shouldThrowExceptionWhenMessageProtoIsNotProvided() {
        expectedException.expect(DaggerSerializationException.class);
        expectedException.expectMessage("messageProtoClassName is required");

        String[] columnNames = {};
        new ProtoSerializer(null, null, columnNames, stencilClientOrchestrator, outputTopic);
    }

    @Test
    public void shouldReturnNullKeyWhenOnlyMessageProtoProvided() {
        String[] columnNames = {"s2_id_level"};
        String protoMessage = "com.gojek.esb.aggregate.demand.AggregatedDemandMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(null, protoMessage, columnNames,
                stencilClientOrchestrator, outputTopic);

        Row element = new Row(1);
        element.setField(0, 13);

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        assertNull(producerRecord.key());
        assertNotNull(producerRecord.value());
    }

    @Test
    public void shouldReturnNullKeyWhenKeyIsEmptyString() {
        String[] columnNames = {"s2_id_level"};
        String protoMessage = "com.gojek.esb.aggregate.demand.AggregatedDemandMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer("", protoMessage, columnNames, stencilClientOrchestrator, outputTopic);

        Row element = new Row(1);
        element.setField(0, 13);

        ProducerRecord<byte[], byte[]> producerRecord = protoSerializer.serialize(element, null);

        assertNull(producerRecord.key());
        assertNotNull(producerRecord.value());
    }

    @Test(expected = DescriptorNotFoundException.class)
    public void shouldThrowDescriptorNotFoundException() {
        String[] columnNames = {"s2_id_level"};
        String protoMessage = "RandomMessageClass";
        ProtoSerializer protoSerializer = new ProtoSerializer(null, protoMessage, columnNames, stencilClientOrchestrator, outputTopic);

        int s2IdLevel = 13;
        Row element = new Row(1);
        element.setField(0, s2IdLevel);

        protoSerializer.serialize(element, null);
    }

    @Test
    public void shouldSerializeKeyWithProvidedProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"s2_id_level"};
        String protoMessage = "com.gojek.esb.aggregate.demand.AggregatedDemandMessage";
        String protoKey = "com.gojek.esb.aggregate.demand.AggregatedDemandKey";
        ProtoSerializer protoSerializer = new ProtoSerializer(protoKey, protoMessage, columnNames, stencilClientOrchestrator, outputTopic);

        int s2IdLevel = 13;
        Row element = new Row(1);
        element.setField(0, s2IdLevel);

        byte[] bytes = protoSerializer.serializeKey(element);
        AggregatedDemandKey actualKey = AggregatedDemandKey.parseFrom(bytes);

        assertEquals(s2IdLevel, actualKey.getS2IdLevel());
    }

    @Test
    public void shouldSerializeMessageWithProvidedProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type", "unique_customers"};
        String outputProtoKey = "com.gojek.esb.aggregate.demand.AggregatedDemandKey";
        String outputProtoMessage = "com.gojek.esb.aggregate.demand.AggregatedDemandMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
        long seconds = System.currentTimeMillis() / 1000;

        Row element = new Row(6);
        Timestamp timestamp = new Timestamp(seconds * 1000);
        com.google.protobuf.Timestamp expectedTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(seconds)
                .setNanos(0)
                .build();

        element.setField(0, timestamp);
        element.setField(1, timestamp);
        element.setField(2, 13);
        element.setField(3, 3322909458387959808L);
        element.setField(4, GO_RIDE);
        element.setField(5, 2L);

        byte[] bytes = protoSerializer.serializeValue(element);

        AggregatedDemandMessage actualValue = AggregatedDemandMessage.parseFrom(bytes);

        assertEquals(expectedTimestamp, actualValue.getWindowStartTime());
        assertEquals(expectedTimestamp, actualValue.getWindowEndTime());
        assertEquals(13, actualValue.getS2IdLevel());
        assertEquals(3322909458387959808L, actualValue.getS2Id());
        assertEquals("GO_RIDE", actualValue.getServiceType().toString());
        assertEquals(2L, actualValue.getUniqueCustomers());
    }

    @Test
    public void shouldThrowExceptionWhenOutputTopicIsNullForSerializeMethod() {
        expectedException.expect(DaggerSerializationException.class);
        expectedException.expectMessage("outputTopic is required");

        String[] columnNames = {"order_number"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, null);
        Row element = new Row(1);
        element.setField(0, "1234");

        protoSerializer.serialize(element, null);
    }

    @Test
    public void shouldThrowExceptionWhenOutputTopicIsEmptyForSerializeMethod() {
        expectedException.expect(DaggerSerializationException.class);
        expectedException.expectMessage("outputTopic is required");

        String[] columnNames = {"order_number"};
        String outputProtoKey = "com.gojek.esb.booking.BookingLogKey";
        String outputProtoMessage = "com.gojek.esb.booking.BookingLogMessage";
        ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, "");
        Row element = new Row(1);
        element.setField(0, "1234");

        protoSerializer.serialize(element, null);
    }
}
