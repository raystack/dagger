package io.odpf.dagger.common.serde.typehandler.complex;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MapEntry;
import com.google.protobuf.WireFormat;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestComplexMap;
import io.odpf.dagger.consumer.TestMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.requiredGroup;
import static org.apache.parquet.schema.Types.requiredMap;
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
        List<MapEntry> entries = (List<MapEntry>) returnedBuilder.getField(mapFieldDescriptor);

        assertEquals(2, entries.size());
        assertEquals("a", entries.get(0).getAllFields().values().toArray()[0]);
        assertEquals("123", entries.get(0).getAllFields().values().toArray()[1]);
        assertEquals("b", entries.get(1).getAllFields().values().toArray()[0]);
        assertEquals("456", entries.get(1).getAllFields().values().toArray()[1]);
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
        List<MapEntry> entries = (List<MapEntry>) returnedBuilder.getField(mapFieldDescriptor);

        assertEquals(2, entries.size());
        assertEquals("a", entries.get(0).getAllFields().values().toArray()[0]);
        assertEquals("123", entries.get(0).getAllFields().values().toArray()[1]);
        assertEquals("b", entries.get(1).getAllFields().values().toArray()[0]);
        assertEquals("456", entries.get(1).getAllFields().values().toArray()[1]);
    }

    @Test
    public void shouldThrowExceptionIfRowsPassedAreNotOfArityTwo() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        ArrayList<Row> inputRows = new ArrayList<>();

        Row inputRow = new Row(3);
        inputRows.add(inputRow);
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                () -> mapHandler.transformToProtoBuilder(builder, inputRows.toArray()));
        assertEquals("Row: +I[null, null, null] of size: 3 cannot be converted to map", exception.getMessage());
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

        assertEquals("a", ((Row) outputValues.get(0)).getField(0));
        assertEquals("123", ((Row) outputValues.get(0)).getField(1));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
        assertEquals("b", ((Row) outputValues.get(1)).getField(0));
        assertEquals("456", ((Row) outputValues.get(1)).getField(1));
        assertEquals(2, ((Row) outputValues.get(1)).getArity());
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

        assertEquals("a", ((Row) outputValues.get(0)).getField(0));
        assertEquals("123", ((Row) outputValues.get(0)).getField(1));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
        assertEquals("b", ((Row) outputValues.get(1)).getField(0));
        assertEquals("456", ((Row) outputValues.get(1)).getField(1));
        assertEquals(2, ((Row) outputValues.get(1)).getArity());
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

        assertEquals(1, ((Row) outputValues.get(0)).getField(0));
        assertEquals("123", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("abc", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
        assertEquals(2, ((Row) outputValues.get(1)).getField(0));
        assertEquals("456", ((Row) ((Row) outputValues.get(1)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(1)).getField(1)).getField(1));
        assertEquals("efg", ((Row) ((Row) outputValues.get(1)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(1)).getArity());
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

        assertEquals(0, ((Row) outputValues.get(0)).getField(0));
        assertEquals("123", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("abc", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
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

        assertEquals(1, ((Row) outputValues.get(0)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
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

        assertEquals(0, ((Row) outputValues.get(0)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
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

        assertEquals(0, ((Row) outputValues.get(0)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
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
    public void shouldReturnArrayOfRowsEachContainingKeysAndValuesForSimpleGroupContainingAMap() {
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
    public void shouldReturnArrayOfRowsEachContainingKeysAndValuesWhenHandlingSimpleGroupContainingMapOfComplexTypes() {
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
    public void shouldReturnEmptyRowArrayWhenHandlingASimpleGroupWithMapFieldNotInitialized() {
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
    public void shouldReturnEmptyRowArrayWhenHandlingNullSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapHandler mapHandler = new MapHandler(fieldDescriptor);

        Row[] actualRows = (Row[]) mapHandler.transformFromParquet(null);

        assertArrayEquals(new Row[0], actualRows);
    }

    @Test
    public void shouldUseDefaultKeyAsPerTypeWhenHandlingSimpleGroupAndTheMapEntryDoesNotHaveKeyInitialized() {
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
    public void shouldUseDefaultValueAsPerTypeWhenHandlingSimpleGroupAndTheMapEntryDoesNotHaveValueInitialized() {
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
}
