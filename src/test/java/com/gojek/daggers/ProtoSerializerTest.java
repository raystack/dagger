package com.gojek.daggers;

import com.gojek.daggers.async.connector.EsResponseHandler;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.esb.aggregate.demand.AggregatedDemandKey;
import com.gojek.esb.aggregate.demand.AggregatedDemandMessage;
import com.gojek.esb.aggregate.supply.AggregatedSupplyKey;
import com.gojek.esb.aggregate.supply.AggregatedSupplyMessage;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.customer.CustomerLogMessage;
import com.gojek.esb.fraud.EnrichedBookingLogMessage;
import com.gojek.esb.types.BookingStatusProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.timgroup.statsd.StatsDClient;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.elasticsearch.client.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.gojek.esb.types.ServiceTypeProto.ServiceType.Enum.GO_RIDE;
import static com.gojek.esb.types.VehicleTypeProto.VehicleType.Enum.BIKE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class ProtoSerializerTest {

    @Test
    public void shouldSerializeKeyForDemandProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type"};
        String protoClassName = "com.gojek.esb.aggregate.demand.AggregatedDemand";
        ProtoSerializer protoSerializer = new ProtoSerializer(protoClassName, columnNames, StencilClientFactory.getClient());
        long seconds = System.currentTimeMillis() / 1000;

        Row element = new Row(5);
        Timestamp timestamp = new java.sql.Timestamp(seconds * 1000);
        com.google.protobuf.Timestamp expectedTimestamp = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(seconds)
                .setNanos(0)
                .build();

        element.setField(0, timestamp);
        element.setField(1, timestamp);
        element.setField(2, 13);
        element.setField(3, 3322909458387959808L);
        element.setField(4, GO_RIDE);

        byte[] serializeKey = protoSerializer.serializeKey(element);

        AggregatedDemandKey actualKey = AggregatedDemandKey.parseFrom(serializeKey);

        Assert.assertEquals(expectedTimestamp, actualKey.getWindowStartTime());
        Assert.assertEquals(expectedTimestamp, actualKey.getWindowEndTime());
        Assert.assertEquals(13, actualKey.getS2IdLevel());
        Assert.assertEquals(3322909458387959808L, actualKey.getS2Id());
        Assert.assertEquals("GO_RIDE", actualKey.getServiceType().toString());
    }

    @Test
    public void shouldSerializeValueForDemandProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "service_type", "unique_customers"};
        String protoClassNamePrefix = "com.gojek.esb.aggregate.demand.AggregatedDemand";
        ProtoSerializer protoSerializer = new ProtoSerializer(protoClassNamePrefix, columnNames, StencilClientFactory.getClient());
        long seconds = System.currentTimeMillis() / 1000;

        Row element = new Row(6);
        Timestamp timestamp = new java.sql.Timestamp(seconds * 1000);
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

        byte[] serializeValue = protoSerializer.serializeValue(element);

        AggregatedDemandMessage actualValue = AggregatedDemandMessage.parseFrom(serializeValue);

        Assert.assertEquals(expectedTimestamp, actualValue.getWindowStartTime());
        Assert.assertEquals(expectedTimestamp, actualValue.getWindowEndTime());
        Assert.assertEquals(13, actualValue.getS2IdLevel());
        Assert.assertEquals(3322909458387959808L, actualValue.getS2Id());
        Assert.assertEquals("GO_RIDE", actualValue.getServiceType().toString());
        Assert.assertEquals(2L, actualValue.getUniqueCustomers());
    }

    @Test
    public void shouldSerializeKeyForSupplyProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "vehicle_type"};
        String protoClassName = "com.gojek.esb.aggregate.supply.AggregatedSupply";
        ProtoSerializer protoSerializer = new ProtoSerializer(protoClassName, columnNames, StencilClientFactory.getClient());
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

        byte[] serializeKey = protoSerializer.serializeKey(element);


        AggregatedSupplyKey actualKey = AggregatedSupplyKey.parseFrom(serializeKey);

        Assert.assertEquals(expectedStartTimestamp, actualKey.getWindowStartTime());
        Assert.assertEquals(expectedEndTimestamp, actualKey.getWindowEndTime());
        Assert.assertEquals(13, actualKey.getS2IdLevel());
        Assert.assertEquals(3322909458387959808L, actualKey.getS2Id());
        Assert.assertEquals("BIKE", actualKey.getVehicleType().toString());
    }

    @Test
    public void shouldSerializeValueForSupplyProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"window_start_time", "window_end_time", "s2_id_level", "s2_id", "vehicle_type", "unique_drivers"};
        String protoClassNamePrefix = "com.gojek.esb.aggregate.supply.AggregatedSupply";
        ProtoSerializer protoSerializer = new ProtoSerializer(protoClassNamePrefix, columnNames, StencilClientFactory.getClient());
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

        byte[] serializeValue = protoSerializer.serializeValue(element);

        AggregatedSupplyMessage actualValue = AggregatedSupplyMessage.parseFrom(serializeValue);

        Assert.assertEquals(expectedStartTimestamp, actualValue.getWindowStartTime());
        Assert.assertEquals(expectedEndTimestamp, actualValue.getWindowEndTime());
        Assert.assertEquals(13, actualValue.getS2IdLevel());
        Assert.assertEquals(3322909458387959808L, actualValue.getS2Id());
        Assert.assertEquals("BIKE", actualValue.getVehicleType().toString());
        Assert.assertEquals(2L, actualValue.getUniqueDrivers());
    }

    @Test
    public void shouldSerializeValueForEnrichedBookingLogProto() throws InvalidProtocolBufferException {
        String[] columnNames = {"service_type", "order_url", "status", "order_number", "event_timestamp", "customer_id", "customer_url", "driver_id", "driver_url", "helooo"};
        String protoClassNamePrefix = "com.gojek.esb.booking.BookingLog";
        ProtoSerializer protoSerializer = new ProtoSerializer(protoClassNamePrefix, columnNames, StencilClientFactory.getClient());
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

        byte[] serializedValue = protoSerializer.serializeValue(element);

        BookingLogMessage bookingLogMessage = BookingLogMessage.parseFrom(serializedValue);

        assertNotNull(bookingLogMessage);
        Assert.assertEquals(timestampProto, bookingLogMessage.getEventTimestamp());
        Assert.assertEquals("12345", bookingLogMessage.getOrderNumber());

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
        System.out.println(customerSource);

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

        long millis = 1537423759;
        com.google.protobuf.Timestamp timestamp1 = com.google.protobuf.Timestamp.newBuilder().setSeconds(millis)
                .setNanos(((int) (millis % 1000) * 1000000)).build();
        System.out.println(timestamp1);

        LocalDate localDate = Instant
                .ofEpochSecond(timestamp1.getSeconds(), timestamp1.getNanos())
                .atZone(ZoneId.of("America/Montreal"))
                .toLocalDate();
        System.out.println(localDate);
    }

    @Test
    public void shouldCreateRowFromInput() throws InvalidProtocolBufferException {
        String[] columnNames = {"service_type", "order_number", "order_url", "status", "event_timestamp", "customer_id", "customer_url", "driver_id", "driver_url"};
        long endTimeInSeconds = (System.currentTimeMillis() + 10000) / 1000;
        Timestamp timestamp = new java.sql.Timestamp(endTimeInSeconds * 1000);

        Row bookingRow = new Row(columnNames.length);
        bookingRow.setField(0, GO_RIDE);
        bookingRow.setField(1, "12345");
        bookingRow.setField(2, "TEST");
        bookingRow.setField(3, BookingStatusProto.BookingStatus.Enum.COMPLETED);
        bookingRow.setField(4, timestamp);
        bookingRow.setField(5, "67890");
        bookingRow.setField(6, "CUST_TEST");
        bookingRow.setField(7, "1234556677");
        bookingRow.setField(8, "DRIVER_TEST");

        String customerProfile = "{\n" +
                " \"_index\": \"customers\",\n" +
                " \"_type\": \"customer\",\n" +
                " \"_id\": \"547774090\",\n" +
                " \"_version\": 11,\n" +
                " \"found\": true,\n" +
                " \"_source\": {\n" +
                "   \"created_at\": \"2015-11-06T12:47:20Z\",\n" +
                "   \"customer_id\": \"547774090\",\n" +
                "   \"email\": \"asfarudin@go-jek.com\",\n" +
//              "   \"is_fb_linked\": false,\n" +
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
        Row enrichedBookingRow = new Row(3);
        String customerProto = "    string customer_id = 1; //required\n" +
                "    string customer_url = 2; //required\n" +
                "    google.protobuf.Timestamp event_timestamp = 3; //required\n" +
                "    string name = 4; //required\n" +
                "    string email = 5; //required\n" +
                "    string phone = 6; //required\n" +
                "    bool phone_verified = 7;\n" +
                "    bool email_verified = 8;\n" +
                "    bool blacklisted = 9;\n" +
                "    bool active = 10;\n" +
                "    string wallet_id = 11;\n" +
                "    gojek.esb.types.GenderType.Enum sex = 12;\n" +
                "    google.protobuf.Timestamp dob = 13;\n" +
                "    string facebook_id = 14;\n" +
                "    bool new_user = 15;\n" +
                "    string signed_up_country = 16;\n" +
                "    string locale = 17;\n" +
                "    google.protobuf.Timestamp created_at = 18;\n" +
                "    google.protobuf.Timestamp updated_at = 19;";
        Row customerRow = new Row(11);
        customerRow.setField(0, "customer_id");
        customerRow.setField(1, "customer_url");
        customerRow.setField(2, timestamp);
        customerRow.setField(3, "customer_name");
        customerRow.setField(4, "customer_email");
        customerRow.setField(5, "customer_phone");
        customerRow.setField(6, true);
        customerRow.setField(7, true);
        customerRow.setField(8, false);
        customerRow.setField(9, true);
        customerRow.setField(10, "wallet_id");

        enrichedBookingRow.setField(0, bookingRow);
        enrichedBookingRow.setField(1, customerRow);
        enrichedBookingRow.setField(2, timestamp);

        String enrichedBookingLogPrefix = "com.gojek.esb.fraud.EnrichedBookingLog";
        ProtoSerializer enrichedBookingLogSerializer = new ProtoSerializer(enrichedBookingLogPrefix, new String[]{"booking_log_message", "customer_profile_message", "event_timestamp"}, StencilClientFactory.getClient());

        byte[] bookingBytes = enrichedBookingLogSerializer.serializeValue(enrichedBookingRow);

        EnrichedBookingLogMessage bookingLog = EnrichedBookingLogMessage.parseFrom(bookingBytes);

        System.out.println(bookingLog);
    }

    @Test
    public void shouldParseData() throws IOException {
        String[] columnNames = {"service_type", "order_number", "order_url", "status", "event_timestamp", "customer_id", "customer_url", "driver_id", "driver_url"};
        long endTimeInSeconds = (System.currentTimeMillis() + 10000) / 1000;
        Timestamp timestamp = new java.sql.Timestamp(endTimeInSeconds * 1000);

        Row bookingRow = new Row(columnNames.length);
        bookingRow.setField(0, GO_RIDE);
        bookingRow.setField(1, "12345");
        bookingRow.setField(2, "TEST");
        bookingRow.setField(3, BookingStatusProto.BookingStatus.Enum.COMPLETED);
        bookingRow.setField(4, timestamp);
        bookingRow.setField(5, "67890");
        bookingRow.setField(6, "CUST_TEST");
        bookingRow.setField(7, "1234556677");
        bookingRow.setField(8, "DRIVER_TEST");

        String customerProfile = "{\"_index\":\"customers\",\"_type\":\"customer\",\"_id\":\"2129\",\"_version\":1,\"found\":true,\"_source\":{\"created_at\":\"2016-01-18T08:55:26.16Z\",\"customer_id\":2129,\"email\":\"mtsalis@ymail.com\",\"is_fb_linked\":false,\"name\":\"salis muhammad\",\"phone\":\"123456789\",\"type\":\"customer\"}}";

        ResultFuture resultFutureMock = mock(ResultFuture.class);
        Descriptors.Descriptor descriptor = CustomerLogMessage.getDescriptor();
        EsResponseHandler esResponseHandler = new EsResponseHandler(mock(StatsDClient.class), bookingRow, resultFutureMock, descriptor, statsManager);


        Response response = mock(Response.class);
        StatusLine statusLineMock = mock(StatusLine.class);
        when(statusLineMock.getStatusCode()).thenReturn(200);
        when(response.getStatusLine()).thenReturn(statusLineMock);
        HttpEntity httpEntity = mock(HttpEntity.class);
        when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(customerProfile.getBytes()));
        when(response.getEntity()).thenReturn(httpEntity);


        esResponseHandler.onSuccess(response);

        Row customerRow = new Row(descriptor.getFields().size());
        customerRow.setField(0, "2129");
        customerRow.setField(3, "salis muhammad");
        customerRow.setField(4, "mtsalis@ymail.com");
        customerRow.setField(5, "123456789");

        Row enrichedBookingRow = new Row(3);
        enrichedBookingRow.setField(0, bookingRow);
        enrichedBookingRow.setField(1, customerRow);
        enrichedBookingRow.setField(2, timestamp);
        verify(resultFutureMock).complete(any());

//      JSONObject jsonObject = new JSONObject(customerProfile);
//      descriptor.get

    }
}


