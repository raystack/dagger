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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class MapTypeHandlerTest {

    @Test
    public void shouldReturnTrueIfMapFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);

        assertTrue(mapTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanMapTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(otherFieldDescriptor);

        assertFalse(mapTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfCannotHandle() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(otherFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(otherFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = mapTypeHandler.transformToProtoBuilder(builder, "123");
        assertEquals("", returnedBuilder.getField(otherFieldDescriptor));
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = mapTypeHandler.transformToProtoBuilder(builder, null);
        List<DynamicMessage> entries = (List<DynamicMessage>) returnedBuilder.getField(mapFieldDescriptor);
        assertEquals(0, entries.size());
    }

    @Test
    public void shouldSetMapFieldIfStringMapPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        DynamicMessage.Builder returnedBuilder = mapTypeHandler.transformToProtoBuilder(builder, inputMap);
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
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
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

        DynamicMessage.Builder returnedBuilder = mapTypeHandler.transformToProtoBuilder(builder, inputRows.toArray());
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
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        ArrayList<Row> inputRows = new ArrayList<>();

        Row inputRow = new Row(3);
        inputRows.add(inputRow);
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                () -> mapTypeHandler.transformToProtoBuilder(builder, inputRows.toArray()));
        assertEquals("Row: +I[null, null, null] of size: 3 cannot be converted to map", exception.getMessage());
    }

    @Test
    public void shouldReturnArrayOfRowHavingSameSizeAsInputMapForTransformForPostProcessor() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromPostProcessor(inputMap));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowHavingFieldsSetAsInputMapAndOfSizeTwoForTransformForPostProcessor() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromPostProcessor(inputMap));

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
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromPostProcessor(null));

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowHavingSameSizeAsInputMapForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        MapEntry<String, String> mapEntry = MapEntry
                .newDefaultInstance(mapFieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
        TestBookingLogMessage driverProfileFlattenLogMessage = TestBookingLogMessage
                .newBuilder()
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("a").setValue("123").buildPartial())
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("b").setValue("456").buildPartial())
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), driverProfileFlattenLogMessage.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowHavingFieldsSetAsInputMapAndOfSizeTwoForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        MapEntry<String, String> mapEntry = MapEntry
                .newDefaultInstance(mapFieldDescriptor.getMessageType(), WireFormat.FieldType.STRING, "", WireFormat.FieldType.STRING, "");
        TestBookingLogMessage driverProfileFlattenLogMessage = TestBookingLogMessage
                .newBuilder()
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("a").setValue("123").buildPartial())
                .addRepeatedField(mapFieldDescriptor, mapEntry.toBuilder().setKey("b").setValue("456").buildPartial())
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), driverProfileFlattenLogMessage.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

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
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        complexMap.put(2, TestMessage.newBuilder().setOrderNumber("456").setOrderDetails("efg").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsAndOfSizeTwoForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        complexMap.put(2, TestMessage.newBuilder().setOrderNumber("456").setOrderDetails("efg").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

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
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.newBuilder().setOrderNumber("123").setOrderDetails("abc").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(0, ((Row) outputValues.get(0)).getField(0));
        assertEquals("123", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("abc", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsIfValueIsDefaultForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(1, TestMessage.getDefaultInstance());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(1, ((Row) outputValues.get(0)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsIfKeyAndValueAreDefaultForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.getDefaultInstance());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(0, ((Row) outputValues.get(0)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
    }

    @Test
    public void shouldReturnArrayOfRowsHavingFieldsSetAsInputMapHavingComplexDataFieldsForDefaultInstanceForTransformForKafka() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestComplexMap.getDescriptor().findFieldByName("complex_map");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        Map<Integer, TestMessage> complexMap = new HashMap<>();
        complexMap.put(0, TestMessage.newBuilder().setOrderNumber("").setOrderDetails("").build());
        TestComplexMap testComplexMap = TestComplexMap.newBuilder().putAllComplexMap(complexMap).build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestComplexMap.getDescriptor(), testComplexMap.toByteArray());

        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromProto(dynamicMessage.getField(mapFieldDescriptor)));

        assertEquals(0, ((Row) outputValues.get(0)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(0));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(1));
        assertEquals("", ((Row) ((Row) outputValues.get(0)).getField(1)).getField(2));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
    }

    @Test
    public void shouldReturnEmptyArrayOfRowIfNullPassedForTransformForKafka() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        List<Object> outputValues = Arrays.asList((Object[]) mapTypeHandler.transformFromPostProcessor(null));

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor mapFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler mapTypeHandler = new MapTypeHandler(mapFieldDescriptor);
        assertEquals(Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{"key", "value"}, Types.STRING, Types.STRING)), mapTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnNullWhenTransformFromParquetIsCalledWithAnyArgument() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        MapTypeHandler protoHandler = new MapTypeHandler(fieldDescriptor);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertNull(protoHandler.transformFromParquet(simpleGroup));
    }
}
