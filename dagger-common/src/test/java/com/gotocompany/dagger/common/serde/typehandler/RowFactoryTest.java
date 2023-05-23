package com.gotocompany.dagger.common.serde.typehandler;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.consumer.TestPrimitiveMessage;
import com.gotocompany.dagger.consumer.TestReason;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.*;

public class RowFactoryTest {

    @Test
    public void shouldCreateRowForInputMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("customer_id", 144614);
        inputMap.put("customer_url", "https://www.abcd.com/1234");
        inputMap.put("active", "true");
        inputMap.put("sex", "male");
        inputMap.put("created_at", "2016-01-18T08:55:26.16Z");
        Row row = RowFactory.createRow(inputMap, descriptor);

        Row expectedRow = new Row(49);
        expectedRow.setField(5, "144614");
        expectedRow.setField(6, "https://www.abcd.com/1234");
        assertEquals(expectedRow, row);

    }

    @Test
    public void shouldReturnAEmptyRowOfSizeEqualToNoOfFieldsInDescriptorForInputMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        Row row = RowFactory.createRow(inputMap, descriptor);
        assertEquals(new Row(49), row);
    }

    @Test
    public void shouldCreateRowWithPassedFieldsForInputMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Map<String, Object> inputMap = new HashMap<>();
        inputMap.put("order_number", "144614");
        inputMap.put("customer_url", "https://www.abcd.com/1234");
        inputMap.put("customer_dynamic_surge_enabled", true);
        inputMap.put("event_timestamp", "2016-01-18T08:55:26.16Z");
        Row row = RowFactory.createRow(inputMap, descriptor);
        assertEquals("144614", row.getField(1));
        assertEquals("https://www.abcd.com/1234", row.getField(6));
        assertEquals(true, row.getField(31));
        assertEquals("2016-01-18T08:55:26.16Z", row.getField(4));
    }

    @Test
    public void shouldReturnEmptyRowIfNullPassedAsMapForInputMap() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Row row = RowFactory.createRow(null, descriptor);
        assertEquals(new Row(49), row);
    }

    @Test
    public void shouldCreateRowForDynamicMessage() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage.newBuilder().build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertNotNull(row);
        assertEquals(49, row.getArity());
    }

    @Test
    public void shouldCreateRowWithPSetFieldsForDynamicMessage() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage
                .newBuilder()
                .setCustomerId("144614")
                .setCustomerUrl("https://www.abcd.com/1234")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertEquals("144614", row.getField(5));
        assertEquals("https://www.abcd.com/1234", row.getField(6));
    }


    @Test
    public void shouldBeAbleToCreateAValidCopyOfTheRowCreated() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage
                .newBuilder()
                .setCustomerId("144614")
                .setCustomerUrl("https://www.abcd.com/1234")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage);
        assertEquals("144614", row.getField(5));
        assertEquals("https://www.abcd.com/1234", row.getField(6));
        ArrayList<TypeInformation<Row>> typeInformations = new ArrayList<>();
        ExecutionConfig config = new ExecutionConfig();
        TestBookingLogMessage.getDescriptor().getFields().forEach(fieldDescriptor -> {
            typeInformations.add(TypeHandlerFactory.getTypeHandler(fieldDescriptor).getTypeInformation());
        });
        ArrayList<TypeSerializer<Row>> typeSerializers = new ArrayList<>();
        typeInformations.forEach(rowTypeInformation -> {
            typeSerializers.add(rowTypeInformation.createSerializer(config));
        });
        RowSerializer rowSerializer = new RowSerializer(typeSerializers.toArray(new TypeSerializer[0]));

        Row copy = rowSerializer.copy(row);

        assertEquals(copy.toString(), row.toString());
    }

    @Test
    public void shouldCreateRowUsingCacheForDynamicMessage() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage.newBuilder().build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        Row row = RowFactory.createRow(dynamicMessage, fieldDescriptorCache);
        assertNotNull(row);
        assertEquals(49, row.getArity());
    }

    @Test
    public void shouldCreateRowUsingCacheWithPSetFieldsForDynamicMessage() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage
                .newBuilder()
                .setCustomerId("144614")
                .setCustomerUrl("https://www.abcd.com/1234")
                .build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        Row row = RowFactory.createRow(dynamicMessage, fieldDescriptorCache);
        assertEquals("144614", row.getField(5));
        assertEquals("https://www.abcd.com/1234", row.getField(6));
    }


    @Test
    public void shouldBeAbleToCreateAValidCopyOfTheRowCreatedUsingCache() throws InvalidProtocolBufferException {
        TestBookingLogMessage customerLogMessage = TestBookingLogMessage
                .newBuilder()
                .setCustomerId("144614")
                .setCustomerUrl("https://www.abcd.com/1234")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), customerLogMessage.toByteArray());
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        Row row = RowFactory.createRow(dynamicMessage, fieldDescriptorCache);
        assertEquals("144614", row.getField(5));
        assertEquals("https://www.abcd.com/1234", row.getField(6));
        ArrayList<TypeInformation<Row>> typeInformations = new ArrayList<>();
        ExecutionConfig config = new ExecutionConfig();
        TestBookingLogMessage.getDescriptor().getFields().forEach(fieldDescriptor -> {
            typeInformations.add(TypeHandlerFactory.getTypeHandler(fieldDescriptor).getTypeInformation());
        });
        ArrayList<TypeSerializer<Row>> typeSerializers = new ArrayList<>();
        typeInformations.forEach(rowTypeInformation -> {
            typeSerializers.add(rowTypeInformation.createSerializer(config));
        });
        RowSerializer rowSerializer = new RowSerializer(typeSerializers.toArray(new TypeSerializer[0]));

        Row copy = rowSerializer.copy(row);

        assertEquals(copy.toString(), row.toString());
    }

    @Test
    public void shouldCreateRowWithPositionIndexingFromSimpleGroup() {
        Descriptors.Descriptor descriptor = TestPrimitiveMessage.getDescriptor();
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BOOLEAN).named("is_valid")
                .required(BINARY).named("order_number")
                .required(BINARY).named("order_hash")
                .required(DOUBLE).named("latitude")
                .required(DOUBLE).named("longitude")
                .required(FLOAT).named("price")
                .required(INT32).named("packet_count")
                .required(INT64).named("phone")
                .required(INT64).named("event_timestamp")
                .required(BINARY).named("service_type")
                .named("TestGroupType");

        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("is_valid", true);
        simpleGroup.add("order_number", "ORDER_1322432");
        String byteString = "g362vxv3ydg73g2ss";
        simpleGroup.add("order_hash", Binary.fromConstantByteArray(byteString.getBytes()));
        simpleGroup.add("latitude", Double.MAX_VALUE);
        simpleGroup.add("longitude", Double.MIN_VALUE);
        simpleGroup.add("price", Float.MAX_VALUE);
        simpleGroup.add("packet_count", Integer.MAX_VALUE);
        simpleGroup.add("phone", Long.MAX_VALUE);
        long currentTimeInMillis = Instant.now().toEpochMilli();
        long seconds = Instant.ofEpochMilli(currentTimeInMillis).getEpochSecond();
        int nanos = Instant.ofEpochMilli(currentTimeInMillis).getNano();
        simpleGroup.add("event_timestamp", currentTimeInMillis);
        simpleGroup.add("service_type", "GO_RIDE");

        Row row = RowFactory.createRow(descriptor, simpleGroup);

        assertNotNull(row);
        assertEquals(10, row.getArity());
        assertEquals(true, row.getField(0));
        assertEquals(Double.MIN_VALUE, row.getField(4));
        Row actualTimestampRow = (Row) row.getField(8);
        assertNotNull(actualTimestampRow);
        assertEquals(seconds, actualTimestampRow.getField(0));
        assertEquals(nanos, actualTimestampRow.getField(1));
    }

    @Test
    public void shouldCreateRowFromSimpleGroupWithExtraFieldsSetToNull() {
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("reason_id")
                .required(BINARY).named("group_id")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("reason_id", "some reason id");
        simpleGroup.add("group_id", "some group id");

        Row actualRow = RowFactory.createRow(TestReason.getDescriptor(), simpleGroup, 2);

        assertEquals(4, actualRow.getArity());
        assertNull(actualRow.getField(2));
        assertNull(actualRow.getField(3));
    }

    @Test
    public void shouldBeAbleToCreateAValidCopyOfTheRowCreatedFromSimpleGroup() {
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("reason_id")
                .required(BINARY).named("group_id")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("reason_id", "some reason id");
        simpleGroup.add("group_id", "some group id");

        Row actualRow = RowFactory.createRow(TestReason.getDescriptor(), simpleGroup);

        ArrayList<TypeInformation<Row>> typeInformations = new ArrayList<>();
        ExecutionConfig config = new ExecutionConfig();
        TestReason.getDescriptor().getFields().forEach(fieldDescriptor -> {
            typeInformations.add(TypeHandlerFactory.getTypeHandler(fieldDescriptor).getTypeInformation());
        });
        ArrayList<TypeSerializer<Row>> typeSerializers = new ArrayList<>();
        typeInformations.forEach(rowTypeInformation -> {
            typeSerializers.add(rowTypeInformation.createSerializer(config));
        });
        RowSerializer rowSerializer = new RowSerializer(typeSerializers.toArray(new TypeSerializer[0]));

        Row copy = rowSerializer.copy(actualRow);

        assertEquals(copy.toString(), actualRow.toString());
    }
}
