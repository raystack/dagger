package com.gojek.daggers.protoHandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.booking.GoFoodBookingLogMessage;
import com.gojek.esb.types.GoFoodShoppingItemProto.GoFoodShoppingItem;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import net.minidev.json.JSONArray;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class RepeatedMessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("routes");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);

        assertTrue(repeatedMesssageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanMessageTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(otherFieldDescriptor);

        assertFalse(repeatedMesssageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals(builder, repeatedMesssageProtoHandler.getProtoBuilder(builder, 123));
        assertEquals("", repeatedMesssageProtoHandler.getProtoBuilder(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForRepeatedMessageFieldTypeDescriptor() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodBookingLogMessage.getDescriptor().findFieldByName("shopping_items");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedMessageFieldDescriptor.getContainingType());

        Row inputRow1 = new Row(9);
        inputRow1.setField(0, 123L);
        inputRow1.setField(2, "pizza");

        Row inputRow2 = new Row(9);
        inputRow2.setField(0, 456L);
        inputRow2.setField(5, "test_id");

        ArrayList<Row> inputRows = new ArrayList<>();
        inputRows.add(inputRow1);
        inputRows.add(inputRow2);

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.getProtoBuilder(builder, inputRows.toArray());

        List<DynamicMessage> returnedShoppingItems = (List<DynamicMessage>) returnedBuilder.getField(repeatedMessageFieldDescriptor);

        GoFoodShoppingItem returnedShoppingItem1 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(0).toByteArray());
        GoFoodShoppingItem returnedShoppingItem2 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(1).toByteArray());

        assertEquals(123L, returnedShoppingItem1.getId());
        assertEquals("pizza", returnedShoppingItem1.getName());
        assertEquals(456L, returnedShoppingItem2.getId());
        assertEquals("test_id", returnedShoppingItem2.getPromoId());
    }

    @Test
    public void shouldSetTheFieldsNotPassedInTheBuilderForRepeatedMessageFieldTypeDescriptorToDefaults() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodBookingLogMessage.getDescriptor().findFieldByName("shopping_items");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedMessageFieldDescriptor.getContainingType());

        Row inputRow1 = new Row(9);
        inputRow1.setField(0, 123L);
        inputRow1.setField(2, "pizza");

        Row inputRow2 = new Row(9);
        inputRow2.setField(0, 456L);
        inputRow2.setField(5, "test_id");

        ArrayList<Row> inputRows = new ArrayList<>();
        inputRows.add(inputRow1);
        inputRows.add(inputRow2);

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.getProtoBuilder(builder, inputRows.toArray());

        List<DynamicMessage> returnedShoppingItems = (List<DynamicMessage>) returnedBuilder.getField(repeatedMessageFieldDescriptor);

        GoFoodShoppingItem returnedShoppingItem1 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(0).toByteArray());
        GoFoodShoppingItem returnedShoppingItem2 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(1).toByteArray());

        assertEquals(0, returnedShoppingItem1.getQuantity());
        assertEquals(0.0D, returnedShoppingItem1.getPrice(), 0.0D);
        assertEquals("", returnedShoppingItem2.getNotes());
        assertEquals("", returnedShoppingItem2.getUuid());
    }

    @Test
    public void shouldNotSetPreviousEntryValuesToFieldsOfNextEntry() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodBookingLogMessage.getDescriptor().findFieldByName("shopping_items");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedMessageFieldDescriptor.getContainingType());

        Row inputRow1 = new Row(9);
        inputRow1.setField(0, 123L);
        inputRow1.setField(2, "pizza");

        Row inputRow2 = new Row(9);
        inputRow2.setField(4, "test_notes");
        inputRow2.setField(5, "test_id");

        ArrayList<Row> inputRows = new ArrayList<>();
        inputRows.add(inputRow1);
        inputRows.add(inputRow2);

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.getProtoBuilder(builder, inputRows.toArray());

        List<DynamicMessage> returnedShoppingItems = (List<DynamicMessage>) returnedBuilder.getField(repeatedMessageFieldDescriptor);

        GoFoodShoppingItem returnedShoppingItem1 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(0).toByteArray());
        GoFoodShoppingItem returnedShoppingItem2 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(1).toByteArray());

        assertEquals(123L, returnedShoppingItem1.getId());
        assertEquals(0L, returnedShoppingItem2.getId());
    }

    @Test
    public void shouldReturnEmptyArrayOfRowsIfNullPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).getTypeAppropriateValue(null);

        assertEquals(0, values.length);
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageOfSameSizeAsDescriptor() {
        JSONArray jsonArray = new JSONArray();

        HashMap<String, Object> inputValues1 = new HashMap<>();
        inputValues1.put("id", 123L);
        inputValues1.put("quantity", 1);
        inputValues1.put("name", "pizza");

        HashMap<String, Object> inputValues2 = new HashMap<>();
        inputValues2.put("id", 456L);
        inputValues2.put("quantity", 2);
        inputValues2.put("name", "pasta");

        jsonArray.appendElement(inputValues1);
        jsonArray.appendElement(inputValues2);

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).getTypeAppropriateValue(jsonArray);

        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[0]).getArity());
        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[1]).getArity());
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageIfFieldsAreLessThanInProto() {
        JSONArray jsonArray = new JSONArray();

        HashMap<String, Object> inputValues1 = new HashMap<>();
        inputValues1.put("id", 123L);
        inputValues1.put("quantity", 1);
        inputValues1.put("name", "pizza");

        HashMap<String, Object> inputValues2 = new HashMap<>();
        inputValues2.put("id", 456L);
        inputValues2.put("quantity", 2);
        inputValues2.put("name", "pasta");

        jsonArray.appendElement(inputValues1);
        jsonArray.appendElement(inputValues2);

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).getTypeAppropriateValue(jsonArray);

        assertEquals(123L, ((Row) values[0]).getField(0));
        assertEquals(1, ((Row) values[0]).getField(1));
        assertEquals("pizza", ((Row) values[0]).getField(2));
        assertEquals(456L, ((Row) values[1]).getField(0));
        assertEquals(2, ((Row) values[1]).getField(1));
        assertEquals("pasta", ((Row) values[1]).getField(2));
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageIfExtraFieldsGiven() {
        JSONArray jsonArray = new JSONArray();

        HashMap<String, Object> inputValues = new HashMap<>();
        inputValues.put("id", 123L);
        inputValues.put("quantity", 1);
        inputValues.put("name", "pizza");
        inputValues.put("random_key", "random_value");


        jsonArray.appendElement(inputValues);

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).getTypeAppropriateValue(jsonArray);

        assertEquals(123L, ((Row) values[0]).getField(0));
        assertEquals(1, ((Row) values[0]).getField(1));
        assertEquals("pizza", ((Row) values[0]).getField(2));
    }

}