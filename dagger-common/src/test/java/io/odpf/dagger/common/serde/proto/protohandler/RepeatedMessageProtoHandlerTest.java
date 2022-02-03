package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestFeedbackLogMessage;
import io.odpf.dagger.consumer.TestReason;
import net.minidev.json.JSONArray;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.Types.OBJECT_ARRAY;
import static org.apache.flink.api.common.typeinfo.Types.ROW_NAMED;
import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepeatedMessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfRepeatedMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageProtoHandler repeatedMessageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);

        assertTrue(repeatedMessageProtoHandler.canHandle());
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

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForRepeatedMessageFieldTypeDescriptor() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageProtoHandler repeatedMessageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedMessageFieldDescriptor.getContainingType());

        Row inputRow1 = new Row(2);
        inputRow1.setField(0, "reason1");
        inputRow1.setField(1, "group1");

        Row inputRow2 = new Row(9);
        inputRow2.setField(0, "reason2");
        inputRow2.setField(1, "group2");

        List<Row> inputRows = new ArrayList<>();
        inputRows.add(inputRow1);
        inputRows.add(inputRow2);

        DynamicMessage.Builder returnedBuilder = repeatedMessageProtoHandler.transformForKafka(builder, inputRows.toArray());

        List<DynamicMessage> reasons = (List<DynamicMessage>) returnedBuilder.getField(repeatedMessageFieldDescriptor);

        TestReason reason1 = TestReason.parseFrom(reasons.get(0).toByteArray());
        TestReason reason2 = TestReason.parseFrom(reasons.get(1).toByteArray());

        assertEquals("reason1", reason1.getReasonId());
        assertEquals("group1", reason1.getGroupId());
        assertEquals("reason2", reason2.getReasonId());
        assertEquals("group2", reason2.getGroupId());
    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForRepeatedMessageFieldTypeDescriptorIfInputIsList() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedMessageFieldDescriptor.getContainingType());

        Row inputRow1 = new Row(2);
        inputRow1.setField(0, "reason1");
        inputRow1.setField(1, "group1");

        Row inputRow2 = new Row(9);
        inputRow2.setField(0, "reason2");
        inputRow2.setField(1, "group2");


        ArrayList<Row> inputRows = new ArrayList<>();
        inputRows.add(inputRow1);
        inputRows.add(inputRow2);

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.transformForKafka(builder, inputRows);


        List<DynamicMessage> reasons = (List<DynamicMessage>) returnedBuilder.getField(repeatedMessageFieldDescriptor);

        TestReason reason1 = TestReason.parseFrom(reasons.get(0).toByteArray());
        TestReason reason2 = TestReason.parseFrom(reasons.get(1).toByteArray());

        assertEquals("reason1", reason1.getReasonId());
        assertEquals("group1", reason1.getGroupId());
        assertEquals("reason2", reason2.getReasonId());
        assertEquals("group2", reason2.getGroupId());
    }


    @Test
    public void shouldSetTheFieldsNotPassedInTheBuilderForRepeatedMessageFieldTypeDescriptorToDefaults() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageProtoHandler repeatedMesssageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedMessageFieldDescriptor.getContainingType());

        Row inputRow1 = new Row(2);
        inputRow1.setField(1, "group1");

        Row inputRow2 = new Row(9);
        inputRow2.setField(0, "reason2");


        ArrayList<Row> inputRows = new ArrayList<>();
        inputRows.add(inputRow1);
        inputRows.add(inputRow2);

        DynamicMessage.Builder returnedBuilder = repeatedMesssageProtoHandler.transformForKafka(builder, inputRows);


        List<DynamicMessage> reasons = (List<DynamicMessage>) returnedBuilder.getField(repeatedMessageFieldDescriptor);

        TestReason reason1 = TestReason.parseFrom(reasons.get(0).toByteArray());
        TestReason reason2 = TestReason.parseFrom(reasons.get(1).toByteArray());

        assertTrue(reason1.getReasonId().isEmpty());
        assertEquals("group1", reason1.getGroupId());
        assertEquals("reason2", reason2.getReasonId());
        assertTrue(reason2.getGroupId().isEmpty());

    }

    @Test
    public void shouldReturnEmptyArrayOfRowsIfNullPassedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(null);

        assertEquals(0, values.length);
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageOfSameSizeAsDescriptorForPostProcessorTransform() {
        JSONArray jsonArray = new JSONArray();

        HashMap<String, Object> inputValues1 = new HashMap<>();
        inputValues1.put("group_id", "group1");
        inputValues1.put("reason_id", "reason1");

        HashMap<String, Object> inputValues2 = new HashMap<>();
        inputValues2.put("group_id", "group2");
        inputValues2.put("reason_id", "reason2");

        jsonArray.appendElement(inputValues1);
        jsonArray.appendElement(inputValues2);

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[0]).getArity());
        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[1]).getArity());
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageIfFieldsAreLessThanInProtoForPostProcessorTransform() {
        JSONArray jsonArray = new JSONArray();

        HashMap<String, Object> inputValues1 = new HashMap<>();
        inputValues1.put("group_id", "group1");
        inputValues1.put("reason_id", "reason1");

        HashMap<String, Object> inputValues2 = new HashMap<>();
        inputValues2.put("group_id", "group2");
        inputValues2.put("reason_id", "reason2");

        jsonArray.appendElement(inputValues1);
        jsonArray.appendElement(inputValues2);

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

        assertEquals("reason1", ((Row) values[0]).getField(0));
        assertEquals("group1", ((Row) values[0]).getField(1));
        assertEquals("reason2", ((Row) values[1]).getField(0));
        assertEquals("group2", ((Row) values[1]).getField(1));
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageIfExtraFieldsGivenForPostProcessorTransform() {
        JSONArray jsonArray = new JSONArray();


        HashMap<String, Object> inputValues = new HashMap<>();
        inputValues.put("group_id", "group1");
        inputValues.put("reason_id", "reason1");
        inputValues.put("random_key", "random_value");


        jsonArray.appendElement(inputValues);

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");

        Object[] values = (Object[]) ProtoHandlerFactory.getProtoHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

        assertEquals("reason1", ((Row) values[0]).getField(0));
        assertEquals("group1", ((Row) values[0]).getField(1));
    }

    @Test
    public void shouldReturnEmptyArrayOfRowsIfNullPassedForKafkaTransform() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");

        Object[] values = (Object[]) new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor).transformFromKafka(null);

        assertEquals(0, values.length);
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageOfAsDescriptorForKafkaTransform() throws InvalidProtocolBufferException {
        TestFeedbackLogMessage logMessage = TestFeedbackLogMessage
                .newBuilder()
                .addReason(TestReason.newBuilder().setReasonId("reason1").setGroupId("group1").build())
                .addReason(TestReason.newBuilder().setReasonId("reason2").setGroupId("group2").build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), logMessage.toByteArray());

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");

        Object[] values = (Object[]) new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor).transformFromKafka(dynamicMessage.getField(repeatedMessageFieldDescriptor));

        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[0]).getArity());
        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[1]).getArity());
        assertEquals("reason1", ((Row) values[0]).getField(0));
        assertEquals("group1", ((Row) values[0]).getField(1));
        assertEquals("reason2", ((Row) values[1]).getField(0));
        assertEquals("group2", ((Row) values[1]).getField(1));
    }


    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageProtoHandler repeatedMessageProtoHandler = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedMessageProtoHandler.getTypeInformation();
        TypeInformation<Row[]> expectedTypeInformation = OBJECT_ARRAY(ROW_NAMED(new String[]{"reason_id", "group_id"}, STRING, STRING));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldConvertRepeatedComplexRowDataToJsonString() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");

        Row inputRow1 = new Row(2);
        inputRow1.setField(0, "reason1");
        inputRow1.setField(1, "group1");

        Row inputRow2 = new Row(2);
        inputRow2.setField(0, "reason2");
        inputRow2.setField(1, "group2");

        Row[] inputRows = new Row[2];
        inputRows[0] = inputRow1;
        inputRows[1] = inputRow2;

        Object value = new RepeatedMessageProtoHandler(repeatedMessageFieldDescriptor).transformToJson(inputRows);
        assertEquals("[{\"reason_id\":\"reason1\",\"group_id\":\"group1\"}, {\"reason_id\":\"reason2\",\"group_id\":\"group2\"}]", String.valueOf(value));
    }

}
