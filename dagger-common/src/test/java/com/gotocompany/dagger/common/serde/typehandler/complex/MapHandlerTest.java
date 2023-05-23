package com.gotocompany.dagger.common.serde.typehandler.complex;

import com.google.protobuf.*;
import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestComplexMap;
import com.gotocompany.dagger.consumer.TestMessage;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.util.*;

import static org.apache.parquet.schema.Types.*;
import static org.junit.Assert.*;

public class MapHandlerTest {

    @Test
    public void shouldReturnTrueIfMapFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);

        assertTrue(mapHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanMapTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MapHandler mapHandler = new MapHandler(otherFieldDescriptor);

        assertFalse(mapHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfCannotHandle() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MapHandler mapHandler = new MapHandler(otherFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(otherFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = mapHandler.transformToProtoBuilder(builder, "123");
        assertEquals("", returnedBuilder.getField(otherFieldDescriptor));
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = mapHandler.transformToProtoBuilder(builder, null);
        List<DynamicMessage> entries = (List<DynamicMessage>) returnedBuilder.getField(mapFieldDescriptor);
        assertEquals(0, entries.size());
    }

    @Test
    public void shouldSetMapFieldIfStringMapPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        DynamicMessage.Builder returnedBuilder = mapHandler.transformToProtoBuilder(builder, inputMap);
        List<DynamicMessage> entries = (List<DynamicMessage>) returnedBuilder.getField(mapFieldDescriptor);

        assertEquals(2, entries.size());
        assertArrayEquals(Arrays.asList("a", "123").toArray(), entries.get(0).getAllFields().values().toArray());
        assertArrayEquals(Arrays.asList("b", "456").toArray(), entries.get(1).getAllFields().values().toArray());
    }

    @Test
    public void shouldSetMapFieldIfArrayofObjectsHavingRowsWithStringFieldsPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        ArrayList<Row> inputRows = new ArrayList<>();

        Row inputRow1 = new Row(2);
        inputRow1.setField(0, "a");
        inputRow1.setField(1, "123");


        Row inputRow2 = new Row(2);
        inputRow2.setField(0, "b");
        inputRow2.setField(1, "456");

        inputRows.add(inputRow1);
        inputRows.add(inputRow2);

        DynamicMessage.Builder returnedBuilder = mapHandler.transformToProtoBuilder(builder, inputRows.toArray());
        List<DynamicMessage> entries = (List<DynamicMessage>) returnedBuilder.getField(mapFieldDescriptor);

        assertEquals(2, entries.size());
        assertArrayEquals(Arrays.asList("a", "123").toArray(), entries.get(0).getAllFields().values().toArray());
        assertArrayEquals(Arrays.asList("b", "456").toArray(), entries.get(1).getAllFields().values().toArray());
    }

    @Test
    public void shouldHandleComplexTypeValuesForSerialization() throws InvalidProtocolBufferException {
        Row inputValue1 = Row.of("12345", Row.of(Arrays.asList("a", "b")));
        Row inputValue2 = Row.of(1234123, Row.of(Arrays.asList("d", "e")));
        Object input = Arrays.asList(inputValue1, inputValue2).toArray();

        Descriptors.FieldDescriptor intMessageDescriptor = TestComplexMap.getDescriptor().findFieldByName("int_message");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(TestComplexMap.getDescriptor());

        byte[] data = new MapHandler(intMessageDescriptor).transformToProtoBuilder(builder, input).build().toByteArray();
        TestComplexMap actualMsg = TestComplexMap.parseFrom(data);
        assertArrayEquals(Arrays.asList(12345L, 1234123L).toArray(), actualMsg.getIntMessageMap().keySet().toArray());
        TestComplexMap.IdMessage idMessage = (TestComplexMap.IdMessage) actualMsg.getIntMessageMap().values().toArray()[0];
        assertTrue(idMessage.getIdsList().containsAll(Arrays.asList("a", "b")));
        idMessage = (TestComplexMap.IdMessage) actualMsg.getIntMessageMap().values().toArray()[1];
        assertTrue(idMessage.getIdsList().containsAll(Arrays.asList("d", "e")));
    }

    @Test
    public void shouldReturnArrayOfRowHavingSameSizeAsInputMapForTransformForPostProcessor() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromPostProcessor(inputMap));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowHavingFieldsSetAsInputMapAndOfSizeTwoForTransformForPostProcessor() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromPostProcessor(inputMap));

        assertEquals(Row.of("a", "123"), outputValues.get(0));
        assertEquals(Row.of("b", "456"), outputValues.get(1));
    }

    @Test
    public void shouldReturnEmptyArrayOfRowIfNullPassedForTransformForPostProcessor() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromPostProcessor(null));

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowHavingSameSizeAsInputMapForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        MapEntry<String, String> mapEntry = MapEntry
                .newDefaultInstance(mapFieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
        TestBookingLogMessage driverProfileFlattenLogMessage = TestBookingLogMessage
                .newBuilder()
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("a").setValue("123").buildPartial())
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("b").setValue("456").buildPartial())
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), driverProfileFlattenLogMessage.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowHavingFieldsSetAsInputMapAndOfSizeTwoForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        MapEntry<String, String> mapEntry = MapEntry
                .newDefaultInstance(mapFieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
        TestBookingLogMessage driverProfileFlattenLogMessage = TestBookingLogMessage
                .newBuilder()
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("a").setValue("123").buildPartial())
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("b").setValue("456").buildPartial())
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), driverProfileFlattenLogMessage.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(Row.of("a", "123"), outputValues.get(0));
        assertEquals(Row.of("b", "456"), outputValues.get(1));
    }

    @Test
    public void shouldReturnArrayOfRowHavingSameSizeAsInputMapHavingComplexDataFieldsForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        complexMap.put(2, TestMessage.newBuilder().setOrderNumber("456").setOrderDetails("efg").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsAndOfSizeTwoForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        complexMap.put(2, TestMessage.newBuilder().setOrderNumber("456").setOrderDetails("efg").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        Row mapEntry1 = Row.of(1, Row.of("123", "", "abc"));
        Row mapEntry2 = Row.of(2, Row.of("456", "", "efg"));

        assertEquals(mapEntry1, outputValues.get(0));
        assertEquals(mapEntry2, outputValues.get(1));
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsIfKeyIsSetAsDefaultProtoValueForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        Row expected = Row.of(0, Row.of("123", "", "abc"));
        assertEquals(expected, outputValues.get(0));
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsIfValueIsDefaultForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.getDefaultInstance());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        Row expected = Row.of(1, Row.of("", "", ""));

        assertEquals(expected, outputValues.get(0));
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsIfKeyAndValueAreDefaultForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.getDefaultInstance());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        Row expected = Row.of(0, Row.of("", "", ""));

        assertEquals(expected, outputValues.get(0));
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsForDefaultInstanceForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.newBuilder().setOrderNumber("").setOrderDetails("").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        Row expected = Row.of(0, Row.of("", "", ""));
        assertEquals(expected, outputValues.get(0));
    }


    @Test
    public void shouldReturnArrayOfRowHavingSameSizeAsInputMapForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        MapEntry<String, String> mapEntry = MapEntry
                .newDefaultInstance(mapFieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
        TestBookingLogMessage driverProfileFlattenLogMessage = TestBookingLogMessage
                .newBuilder()
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("a").setValue("123").buildPartial())
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("b").setValue("456").buildPartial())
                .build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), driverProfileFlattenLogMessage.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProtoUsingCache(dynamicMessage.getField(mapFieldDescriptor), fieldDescriptorCache));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowHavingFieldsSetAsInputMapAndOfSizeTwoForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        MapEntry<String, String> mapEntry = MapEntry
                .newDefaultInstance(mapFieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
        TestBookingLogMessage driverProfileFlattenLogMessage = TestBookingLogMessage
                .newBuilder()
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("a").setValue("123").buildPartial())
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("b").setValue("456").buildPartial())
                .build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), driverProfileFlattenLogMessage.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProtoUsingCache(dynamicMessage.getField(mapFieldDescriptor), fieldDescriptorCache));

        assertEquals(Row.of("a", "123"), outputValues.get(0));
        assertEquals(Row.of("b", "456"), outputValues.get(1));
    }

    @Test
    public void shouldReturnArrayOfRowHavingSameSizeAsInputMapHavingComplexDataFieldsForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        complexMap.put(2, TestMessage.newBuilder().setOrderNumber("456").setOrderDetails("efg").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestComplexMap.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProtoUsingCache(dynamicMessage.getField(mapFieldDescriptor), fieldDescriptorCache));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsAndOfSizeTwoForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        complexMap.put(2, TestMessage.newBuilder().setOrderNumber("456").setOrderDetails("efg").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestComplexMap.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProtoUsingCache(dynamicMessage.getField(mapFieldDescriptor), fieldDescriptorCache));

        Row mapEntry1 = Row.of(1, Row.of("123", "", "abc"));
        Row mapEntry2 = Row.of(2, Row.of("456", "", "efg"));

        assertEquals(mapEntry1, outputValues.get(0));
        assertEquals(mapEntry2, outputValues.get(1));
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsIfKeyIsSetAsDefaultProtoValueForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestComplexMap.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProtoUsingCache(dynamicMessage.getField(mapFieldDescriptor), fieldDescriptorCache));

        Row expected = Row.of(0, Row.of("123", "", "abc"));
        assertEquals(expected, outputValues.get(0));
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsIfValueIsDefaultForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.getDefaultInstance());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestComplexMap.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProtoUsingCache(dynamicMessage.getField(mapFieldDescriptor), fieldDescriptorCache));

        Row expected = Row.of(1, Row.of("", "", ""));

        assertEquals(expected, outputValues.get(0));
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsIfKeyAndValueAreDefaultForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.getDefaultInstance());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestComplexMap.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProtoUsingCache(dynamicMessage.getField(mapFieldDescriptor), fieldDescriptorCache));

        Row expected = Row.of(0, Row.of("", "", ""));

        assertEquals(expected, outputValues.get(0));
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsForDefaultInstanceForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.newBuilder().setOrderNumber("").setOrderDetails("").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestComplexMap.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromProtoUsingCache(dynamicMessage.getField(mapFieldDescriptor), fieldDescriptorCache));

        Row expected = Row.of(0, Row.of("", "", ""));
        assertEquals(expected, outputValues.get(0));
    }

    @Test
    public void shouldReturnEmptyArrayOfRowIfNullPassedForTransformForKafka() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        List<Object> outputValues = Arrays.asList((Object[]) mapHandler.transformFromPostProcessor(null));

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        assertEquals(Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{"key", "value"}, Types.STRING, Types.STRING)), mapHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayOfRowsForSimpleGroupContainingStandardSpecMap() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType keyValueSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("key")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("value")
                .named("key_value");
        GroupType mapSchema = requiredMap()
                .key(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .requiredValue(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .named("metadata");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestBookingLogMessage");

        SimpleGroup keyValue1 = new SimpleGroup(keyValueSchema);
        keyValue1.add("key", "batman");
        keyValue1.add("value", "DC");
        SimpleGroup keyValue2 = new SimpleGroup(keyValueSchema);
        keyValue2.add("key", "starlord");
        keyValue2.add("value", "Marvel");

        SimpleGroup mapMessage = new SimpleGroup(mapSchema);
        mapMessage.add("key_value", keyValue1);
        mapMessage.add("key_value", keyValue2);

        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("metadata", mapMessage);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);
        Row[] expectedRows = new Row[]{Row.of("batman", "DC"), Row.of("starlord", "Marvel")};

        assertEquals(2, actualRows.length);
        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldReturnArrayOfRowsForSimpleGroupContainingLegacySpecMap() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType mapSchema = repeatedGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .named("metadata");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestBookingLogMessage");

        SimpleGroup keyValue1 = new SimpleGroup(mapSchema);
        keyValue1.add("key", "batman");
        keyValue1.add("value", "DC");
        SimpleGroup keyValue2 = new SimpleGroup(mapSchema);
        keyValue2.add("key", "starlord");
        keyValue2.add("value", "Marvel");


        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("metadata", keyValue1);
        mainMessage.add("metadata", keyValue2);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);
        Row[] expectedRows = new Row[]{Row.of("batman", "DC"), Row.of("starlord", "Marvel")};

        assertEquals(2, actualRows.length);
        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldReturnArrayOfRowsWhenHandlingSimpleGroupContainingStandardSpecMapOfComplexTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType valueSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_number")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_url")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_details")
                .named("value");
        GroupType keyValueSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("key")
                .addField(valueSchema)
                .named("key_value");
        GroupType mapSchema = requiredMap()
                .key(PrimitiveType.PrimitiveTypeName.INT32)
                .value(valueSchema)
                .named("complex_map");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestComplexMap");

        SimpleGroup keyValue1 = new SimpleGroup(keyValueSchema);
        SimpleGroup value1 = new SimpleGroup(valueSchema);
        value1.add("order_number", "RS-123");
        value1.add("order_url", "http://localhost");
        value1.add("order_details", "some-details");
        keyValue1.add("key", 10);
        keyValue1.add("value", value1);

        SimpleGroup keyValue2 = new SimpleGroup(keyValueSchema);
        SimpleGroup value2 = new SimpleGroup(valueSchema);
        value2.add("order_number", "RS-456");
        value2.add("order_url", "http://localhost:8888/some-url");
        value2.add("order_details", "extra-details");
        keyValue2.add("key", 90);
        keyValue2.add("value", value2);

        SimpleGroup mapMessage = new SimpleGroup(mapSchema);
        mapMessage.add("key_value", keyValue1);
        mapMessage.add("key_value", keyValue2);

        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("complex_map", mapMessage);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);
        Row[] expectedRows = new Row[]{
                Row.of(10, Row.of("RS-123", "http://localhost", "some-details")),
                Row.of(90, Row.of("RS-456", "http://localhost:8888/some-url", "extra-details"))};

        assertEquals(2, actualRows.length);
        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldReturnArrayOfRowsWhenHandlingSimpleGroupContainingLegacySpecMapOfComplexTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType valueSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_number")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_url")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_details")
                .named("value");
        GroupType mapSchema = repeatedGroup()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("key")
                .addField(valueSchema)
                .named("complex_map");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestComplexMap");

        SimpleGroup keyValue1 = new SimpleGroup(mapSchema);
        SimpleGroup value1 = new SimpleGroup(valueSchema);
        value1.add("order_number", "RS-123");
        value1.add("order_url", "http://localhost");
        value1.add("order_details", "some-details");
        keyValue1.add("key", 10);
        keyValue1.add("value", value1);

        SimpleGroup keyValue2 = new SimpleGroup(mapSchema);
        SimpleGroup value2 = new SimpleGroup(valueSchema);
        value2.add("order_number", "RS-456");
        value2.add("order_url", "http://localhost:8888/some-url");
        value2.add("order_details", "extra-details");
        keyValue2.add("key", 90);
        keyValue2.add("value", value2);

        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("complex_map", keyValue1);
        mainMessage.add("complex_map", keyValue2);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);
        Row[] expectedRows = new Row[]{
                Row.of(10, Row.of("RS-123", "http://localhost", "some-details")),
                Row.of(90, Row.of("RS-456", "http://localhost:8888/some-url", "extra-details"))};

        assertEquals(2, actualRows.length);
        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldReturnEmptyRowArrayWhenHandlingASimpleGroupNotContainingTheMapField() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);
        MessageType parquetSchema = buildMessage()
                .named("TestBookingLogMessage");
        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);

        assertArrayEquals(new Row[0], actualRows);
    }

    @Test
    public void shouldReturnEmptyRowArrayWhenHandlingASimpleGroupWithStandardSpecMapFieldNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType mapSchema = requiredMap()
                .key(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .requiredValue(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .named("metadata");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestBookingLogMessage");
        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);

        assertArrayEquals(new Row[0], actualRows);
    }

    @Test
    public void shouldReturnEmptyRowArrayWhenHandlingASimpleGroupWithLegacySpecMapFieldNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType mapSchema = repeatedGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .named("metadata");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestBookingLogMessage");
        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);

        assertArrayEquals(new Row[0], actualRows);
    }


    @Test
    public void shouldReturnEmptyRowArrayWhenHandlingNullSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(null);

        assertArrayEquals(new Row[0], actualRows);
    }

    @Test
    public void shouldUseDefaultKeyAsPerTypeWhenHandlingSimpleGroupAndStandardSpecMapEntryDoesNotHaveKeyInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType keyValueSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("key")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("value")
                .named("key_value");
        GroupType mapSchema = requiredMap()
                .key(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .requiredValue(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                .named("metadata");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestBookingLogMessage");

        /* Creating a map entry and only initializing the value but not the key */
        SimpleGroup keyValue = new SimpleGroup(keyValueSchema);
        keyValue.add("value", "DC");

        SimpleGroup mapMessage = new SimpleGroup(mapSchema);
        mapMessage.add("key_value", keyValue);

        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("metadata", mapMessage);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);
        Row[] expectedRows = new Row[]{Row.of("", "DC")};

        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldUseDefaultKeyAsPerTypeWhenHandlingSimpleGroupAndLegacySpecMapEntryDoesNotHaveKeyInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType mapSchema = repeatedGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .named("metadata");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestBookingLogMessage");

        /* Creating a map entry and only initializing the value but not the key */
        SimpleGroup keyValue = new SimpleGroup(mapSchema);
        keyValue.add("value", "DC");

        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("metadata", keyValue);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);
        Row[] expectedRows = new Row[]{Row.of("", "DC")};

        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldUseDefaultValueAsPerTypeWhenHandlingSimpleGroupAndStandardSpecMapEntryDoesNotHaveValueInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType valueSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_number")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_url")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_details")
                .named("value");
        GroupType keyValueSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("key")
                .addField(valueSchema)
                .named("key_value");
        GroupType mapSchema = requiredMap()
                .key(PrimitiveType.PrimitiveTypeName.INT32)
                .value(valueSchema)
                .named("complex_map");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestComplexMap");

        SimpleGroup keyValue = new SimpleGroup(keyValueSchema);
        keyValue.add("key", 10);

        /* Just creating an empty simple group for the value, without initializing any of the fields in it */
        SimpleGroup value = new SimpleGroup(valueSchema);
        keyValue.add("value", value);

        SimpleGroup mapMessage = new SimpleGroup(mapSchema);
        mapMessage.add("key_value", keyValue);

        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("complex_map", mapMessage);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);
        Row[] expectedRows = new Row[]{
                Row.of(10, Row.of("", "", ""))};

        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldUseDefaultValueAsPerTypeWhenHandlingSimpleGroupAndLegacySpecMapEntryDoesNotHaveValueInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType mapSchema = repeatedGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("value")
                .named("metadata");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestBookingLogMessage");

        /* Creating a map entry and only initializing the key but not the value */
        SimpleGroup keyValue = new SimpleGroup(mapSchema);
        keyValue.add("key", "Superman");

        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("metadata", keyValue);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);
        Row[] expectedRows = new Row[]{Row.of("Superman", "")};

        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldReturnEmptyRowArrayWhenHandlingSimpleGroupAndMapFieldDoesNotConformWithAnySpec() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        GroupType mapSchema = repeatedGroup()
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("random_key")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("random_value")
                .named("metadata");
        MessageType parquetSchema = buildMessage()
                .addField(mapSchema)
                .named("TestBookingLogMessage");

        SimpleGroup keyValue = new SimpleGroup(mapSchema);
        keyValue.add("random_key", "Superman");
        keyValue.add("random_value", "DC");

        SimpleGroup mainMessage = new SimpleGroup(parquetSchema);
        mainMessage.add("metadata", keyValue);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(mainMessage);

        assertArrayEquals(new Row[]{}, actualRows);
    }
}
