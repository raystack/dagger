package io.odpf.dagger.protohandler;

import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import net.minidev.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static org.apache.flink.api.common.typeinfo.Types.BOOLEAN;
import static org.apache.flink.api.common.typeinfo.Types.DOUBLE;
import static org.apache.flink.api.common.typeinfo.Types.INT;
import static org.apache.flink.api.common.typeinfo.Types.LONG;
import static org.apache.flink.api.common.typeinfo.Types.OBJECT_ARRAY;
import static org.apache.flink.api.common.typeinfo.Types.ROW_NAMED;
import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepeatedMessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfRepeatedMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("routes");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);

        assertTrue(repeatedMesssageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanRepeatedMessageTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(otherFieldDescriptor);

        assertFalse(repeatedMesssageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals(builder, repeatedMesssageProtoHandler.transformForKafka(builder, 123));
        assertEquals("", repeatedMesssageProtoHandler.transformForKafka(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfNullPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder outputBuilder = repeatedMesssageProtoHandler.transformForKafka(builder, null);
        assertEquals(builder, outputBuilder);
        assertEquals("", outputBuilder.getField(fieldDescriptor));
    }

    /*
    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForRepeatedMessageFieldTypeDescriptor() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");
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

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.transformForKafka(builder, inputRows.toArray());

        List<DynamicMessage> returnedShoppingItems = (List<DynamicMessage>) returnedBuilder.getField(repeatedMessageFieldDescriptor);

        GoFoodShoppingItem returnedShoppingItem1 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(0).toByteArray());
        GoFoodShoppingItem returnedShoppingItem2 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(1).toByteArray());

        assertEquals(123L, returnedShoppingItem1.getId());
        assertEquals("pizza", returnedShoppingItem1.getName());
        assertEquals(456L, returnedShoppingItem2.getId());
        assertEquals("test_id", returnedShoppingItem2.getPromoId());
    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForRepeatedMessageFieldTypeDescriptorIfInputIsList() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodTestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");
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

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.transformForKafka(builder, inputRows);

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
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = GoFoodTestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");
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

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.transformForKafka(builder, inputRows.toArray());

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
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");
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

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.transformForKafka(builder, inputRows.toArray());

        List<DynamicMessage> returnedShoppingItems = (List<DynamicMessage>) returnedBuilder.getField(repeatedMessageFieldDescriptor);

        GoFoodShoppingItem returnedShoppingItem1 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(0).toByteArray());
        GoFoodShoppingItem returnedShoppingItem2 = GoFoodShoppingItem.parseFrom(returnedShoppingItems.get(1).toByteArray());

        assertEquals(123L, returnedShoppingItem1.getId());
        assertEquals(0L, returnedShoppingItem2.getId());
    }

     */

    @Test
    public void shouldReturnEmptyArrayOfRowsIfNullPassedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(null);

        assertEquals(0, values.length);
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageOfSameSizeAsDescriptorForPostProcessorTransform() {
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

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[0]).getArity());
        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[1]).getArity());
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageIfFieldsAreLessThanInProtoForPostProcessorTransform() {
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

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

        assertEquals(123L, ((Row) values[0]).getField(0));
        assertEquals(1, ((Row) values[0]).getField(1));
        assertEquals("pizza", ((Row) values[0]).getField(2));
        assertEquals(456L, ((Row) values[1]).getField(0));
        assertEquals(2, ((Row) values[1]).getField(1));
        assertEquals("pasta", ((Row) values[1]).getField(2));
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageIfExtraFieldsGivenForPostProcessorTransform() {
        JSONArray jsonArray = new JSONArray();

        HashMap<String, Object> inputValues = new HashMap<>();
        inputValues.put("id", 123L);
        inputValues.put("quantity", 1);
        inputValues.put("name", "pizza");
        inputValues.put("random_key", "random_value");


        jsonArray.appendElement(inputValues);

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

        assertEquals(123L, ((Row) values[0]).getField(0));
        assertEquals(1, ((Row) values[0]).getField(1));
        assertEquals("pizza", ((Row) values[0]).getField(2));
    }

    @Test
    public void shouldReturnEmptyArrayOfRowsIfNullPassedForKafkaTransform() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor).transformFromKafka(null);

        assertEquals(0, values.length);
    }

    /*
    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageOfAsDescriptorForKafkaTransform() throws InvalidProtocolBufferException {
        TestBookingLogMessage goFoodTestBookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .addShoppingItems(GoFoodShoppingItem.newBuilder().setId(123L).setQuantity(1).setName("pizza").build())
                .addShoppingItems(GoFoodShoppingItem.newBuilder().setId(456L).setQuantity(2).setName("pasta").build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), goFoodTestBookingLogMessage.toByteArray());

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Object[] values = (Object[]) new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor).transformFromKafka(dynamicMessage.getField(repeatedMessageFieldDescriptor));

        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[0]).getArity());
        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[1]).getArity());
        assertEquals(123L, ((Row) values[0]).getField(0));
        assertEquals(1, ((Row) values[0]).getField(1));
        assertEquals("pizza", ((Row) values[0]).getField(2));
        assertEquals(456L, ((Row) values[1]).getField(0));
        assertEquals(2, ((Row) values[1]).getField(1));
        assertEquals("pasta", ((Row) values[1]).getField(2));
    }

     */

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");
        RepeatedMessageProtoHandler repeatedMessageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedMessageProtoHandler.getTypeInformation();
        TypeInformation<Row[]> expectedTypeInformation = OBJECT_ARRAY(ROW_NAMED(new String[]{"id", "quantity", "name", "price", "notes", "promo_id", "uuid", "out_of_stock", "variants"},
                LONG, INT, STRING, DOUBLE, STRING, STRING, STRING, BOOLEAN,
                OBJECT_ARRAY(ROW_NAMED(new String[]{"id", "name", "catagory_id", "catagory_name", "out_of_stock"}, STRING, STRING, STRING, STRING, BOOLEAN))));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldConvertRepeatedComplexRowDataToJsonString() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("shopping_items");

        Row inputRow1 = new Row(9);
        inputRow1.setField(0, 123L);
        inputRow1.setField(2, "pizza");

        Row inputRow2 = new Row(9);
        inputRow2.setField(0, 456L);
        inputRow2.setField(5, "test_id");

        Row[] inputRows = new Row[2];
        inputRows[0] = inputRow1;
        inputRows[1] = inputRow2;

        Object value = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor).transformToJson(inputRows);
        Assert.assertEquals("[{\"id\":123,\"quantity\":null,\"name\":\"pizza\",\"price\":null,\"notes\":null,\"promo_id\":null,\"uuid\":null,\"out_of_stock\":null,\"variants\":null}, {\"id\":456,\"quantity\":null,\"name\":null,\"price\":null,\"notes\":null,\"promo_id\":\"test_id\",\"uuid\":null,\"out_of_stock\":null,\"variants\":null}]", String.valueOf(value));
    }
}
