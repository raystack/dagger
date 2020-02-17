package com.gojek.daggers.protoHandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.fraud.DriverProfileFlattenLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MapEntry;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class MapProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfMapFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = DriverProfileFlattenLogMessage.getDescriptor().findFieldByName("metadata");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(mapFieldDescriptor);

        assertTrue(mapProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanMapTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(otherFieldDescriptor);

        assertFalse(mapProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfCannotHandle() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(otherFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(otherFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = mapProtoHandler.getProtoBuilder(builder, "123");
        assertEquals("", returnedBuilder.getField(otherFieldDescriptor));
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = DriverProfileFlattenLogMessage.getDescriptor().findFieldByName("metadata");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = mapProtoHandler.getProtoBuilder(builder, null);
        List<DynamicMessage> entries = (List<DynamicMessage>) returnedBuilder.getField(mapFieldDescriptor);
        assertEquals(entries.size(), 0);
    }

    @Test
    public void shouldSetMapFieldIfStringMapPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = DriverProfileFlattenLogMessage.getDescriptor().findFieldByName("metadata");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        DynamicMessage.Builder returnedBuilder = mapProtoHandler.getProtoBuilder(builder, inputMap);
        List<MapEntry> entries = (List<MapEntry>) returnedBuilder.getField(mapFieldDescriptor);

        assertEquals(2, entries.size());
        assertEquals("a", entries.get(0).getAllFields().values().toArray()[0]);
        assertEquals("123", entries.get(0).getAllFields().values().toArray()[1]);
        assertEquals("b", entries.get(1).getAllFields().values().toArray()[0]);
        assertEquals("456", entries.get(1).getAllFields().values().toArray()[1]);
    }

    @Test
    public void shouldSetMapFieldIfArrayofObjectsHavingRowsWithStringFieldsPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = DriverProfileFlattenLogMessage.getDescriptor().findFieldByName("metadata");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(mapFieldDescriptor);
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

        DynamicMessage.Builder returnedBuilder = mapProtoHandler.getProtoBuilder(builder, inputRows.toArray());
        List<MapEntry> entries = (List<MapEntry>) returnedBuilder.getField(mapFieldDescriptor);

        assertEquals(2, entries.size());
        assertEquals("a", entries.get(0).getAllFields().values().toArray()[0]);
        assertEquals("123", entries.get(0).getAllFields().values().toArray()[1]);
        assertEquals("b", entries.get(1).getAllFields().values().toArray()[0]);
        assertEquals("456", entries.get(1).getAllFields().values().toArray()[1]);
    }

    @Test
    public void shouldThrowExceptionIfRowsPassedAreNotOfArityTwo() {
        Descriptors.FieldDescriptor mapFieldDescriptor = DriverProfileFlattenLogMessage.getDescriptor().findFieldByName("metadata");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(mapFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(mapFieldDescriptor.getContainingType());

        ArrayList<Row> inputRows = new ArrayList<>();

        Row inputRow = new Row(3);
        inputRows.add(inputRow);
        try {
            mapProtoHandler.getProtoBuilder(builder, inputRows.toArray());
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("Row: null,null,null of size: 3 cannot be converted to map", e.getMessage());
        }
    }

    @Test
    public void shouldReturnArrayOfRowHavingSameSizeAsInputMap() {
        Descriptors.FieldDescriptor mapFieldDescriptor = DriverProfileFlattenLogMessage.getDescriptor().findFieldByName("metadata");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(mapFieldDescriptor);
        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        List<Object> outputValues = Arrays.asList((Object[]) mapProtoHandler.getTypeAppropriateValue(inputMap));

        assertEquals(2, outputValues.size());
    }

    @Test
    public void shouldReturnArrayOfRowHavingFieldsSetAsInputMapAndOfSizeTwo() {
        Descriptors.FieldDescriptor mapFieldDescriptor = DriverProfileFlattenLogMessage.getDescriptor().findFieldByName("metadata");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(mapFieldDescriptor);
        HashMap<String, String> inputMap = new HashMap<>();
        inputMap.put("a", "123");
        inputMap.put("b", "456");

        List<Object> outputValues = Arrays.asList((Object[]) mapProtoHandler.getTypeAppropriateValue(inputMap));

        assertEquals("a", ((Row) outputValues.get(0)).getField(0));
        assertEquals("123", ((Row) outputValues.get(0)).getField(1));
        assertEquals(2, ((Row) outputValues.get(0)).getArity());
        assertEquals("b", ((Row) outputValues.get(1)).getField(0));
        assertEquals("456", ((Row) outputValues.get(1)).getField(1));
        assertEquals(2, ((Row) outputValues.get(1)).getArity());
    }

    @Test
    public void shouldReturnEmptyArrayOfRowIfNullPassed() {
        Descriptors.FieldDescriptor mapFieldDescriptor = DriverProfileFlattenLogMessage.getDescriptor().findFieldByName("metadata");
        MapProtoHandler mapProtoHandler = new MapProtoHandler(mapFieldDescriptor);
        List<Object> outputValues = Arrays.asList((Object[]) mapProtoHandler.getTypeAppropriateValue(null));

        assertEquals(0, outputValues.size());
    }

}