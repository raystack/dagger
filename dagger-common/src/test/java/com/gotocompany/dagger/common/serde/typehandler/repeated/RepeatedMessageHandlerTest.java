package com.gotocompany.dagger.common.serde.typehandler.repeated;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandlerFactory;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestFeedbackLogMessage;
import com.gotocompany.dagger.consumer.TestReason;
import net.minidev.json.JSONArray;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.repeatedGroup;
import static org.junit.Assert.*;

public class RepeatedMessageHandlerTest {

    @Test
    public void shouldReturnTrueIfRepeatedMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(repeatedMessageFieldDescriptor);

        assertTrue(repeatedMessageHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanRepeatedMessageTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(otherFieldDescriptor);

        assertFalse(repeatedMessageHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals(builder, repeatedMessageHandler.transformToProtoBuilder(builder, 123));
        assertEquals("", repeatedMessageHandler.transformToProtoBuilder(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfNullPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder outputBuilder = repeatedMessageHandler.transformToProtoBuilder(builder, null);
        assertEquals(builder, outputBuilder);
        assertEquals("", outputBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForRepeatedMessageFieldTypeDescriptor() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(repeatedMessageFieldDescriptor);
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

        DynamicMessage.Builder returnedBuilder = repeatedMessageHandler.transformToProtoBuilder(builder, inputRows.toArray());

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
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(repeatedMessageFieldDescriptor);
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

        DynamicMessage.Builder returnedBuilder = repeatedMessageHandler.transformToProtoBuilder(builder, inputRows);


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
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(repeatedMessageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedMessageFieldDescriptor.getContainingType());

        Row inputRow1 = new Row(2);
        inputRow1.setField(1, "group1");

        Row inputRow2 = new Row(9);
        inputRow2.setField(0, "reason2");


        ArrayList<Row> inputRows = new ArrayList<>();
        inputRows.add(inputRow1);
        inputRows.add(inputRow2);

        DynamicMessage.Builder returnedBuilder = repeatedMessageHandler.transformToProtoBuilder(builder, inputRows);


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
        Object[] values = (Object[]) TypeHandlerFactory.getTypeHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(null);

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

        Object[] values = (Object[]) TypeHandlerFactory.getTypeHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

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

        Object[] values = (Object[]) TypeHandlerFactory.getTypeHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

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

        Object[] values = (Object[]) TypeHandlerFactory.getTypeHandler(repeatedMessageFieldDescriptor).transformFromPostProcessor(jsonArray);

        assertEquals("reason1", ((Row) values[0]).getField(0));
        assertEquals("group1", ((Row) values[0]).getField(1));
    }

    @Test
    public void shouldReturnEmptyArrayOfRowsIfNullPassedForTransformFromProto() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");

        Object[] values = (Object[]) new RepeatedMessageHandler(repeatedMessageFieldDescriptor).transformFromProto(null);

        assertEquals(0, values.length);
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageOfAsDescriptorForTransformFromProto() throws InvalidProtocolBufferException {
        TestFeedbackLogMessage logMessage = TestFeedbackLogMessage
                .newBuilder()
                .addReason(TestReason.newBuilder().setReasonId("reason1").setGroupId("group1").build())
                .addReason(TestReason.newBuilder().setReasonId("reason2").setGroupId("group2").build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), logMessage.toByteArray());

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");

        Object[] values = (Object[]) new RepeatedMessageHandler(repeatedMessageFieldDescriptor).transformFromProto(dynamicMessage.getField(repeatedMessageFieldDescriptor));

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
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(repeatedMessageFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedMessageHandler.getTypeInformation();
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

        Object value = new RepeatedMessageHandler(repeatedMessageFieldDescriptor).transformToJson(inputRows);
        assertEquals("[{\"reason_id\":\"reason1\",\"group_id\":\"group1\"}, {\"reason_id\":\"reason2\",\"group_id\":\"group2\"}]", String.valueOf(value));
    }

    @Test
    public void shouldReturnArrayOfRowsWhenTransformFromParquetIsCalledWithSimpleGroupContainingRepeatedSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(fieldDescriptor);

        GroupType nestedGroupSchema = repeatedGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("reason_id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("group_id")
                .named("reason");
        SimpleGroup value1 = new SimpleGroup(nestedGroupSchema);
        value1.add("reason_id", "FIRST");
        value1.add("group_id", "1234XXXX");
        SimpleGroup value2 = new SimpleGroup(nestedGroupSchema);
        value2.add("reason_id", "SECOND");
        value2.add("group_id", "6789XXXX");

        GroupType mainMessageSchema = buildMessage().addField(nestedGroupSchema).named("MainMessage");
        SimpleGroup mainMessageGroup = new SimpleGroup(mainMessageSchema);
        mainMessageGroup.add("reason", value1);
        mainMessageGroup.add("reason", value2);

        Row[] actualRows = (Row[]) repeatedMessageHandler.transformFromParquet(mainMessageGroup);

        Row expectedRow1 = new Row(2);
        expectedRow1.setField(0, "FIRST");
        expectedRow1.setField(1, "1234XXXX");
        Row expectedRow2 = new Row(2);
        expectedRow2.setField(0, "SECOND");
        expectedRow2.setField(1, "6789XXXX");
        Row[] expectedRows = new Row[]{expectedRow1, expectedRow2};

        assertArrayEquals(expectedRows, actualRows);
    }

    @Test
    public void shouldReturnEmptyRowArrayWhenTransformFromParquetIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(fieldDescriptor);

        Row[] actualRows = (Row[]) repeatedMessageHandler.transformFromParquet(null);

        assertEquals(0, actualRows.length);
    }

    @Test
    public void shouldReturnEmptyRowArrayWhenTransformFromParquetIsCalledWithSimpleGroupAndRepeatedSimpleGroupFieldIsNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(fieldDescriptor);

        GroupType nestedGroupSchema = repeatedGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("reason_id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("group_id")
                .named("reason");

        GroupType mainMessageSchema = buildMessage().addField(nestedGroupSchema).named("MainMessage");
        SimpleGroup mainMessageGroup = new SimpleGroup(mainMessageSchema);

        Row[] actualRows = (Row[]) repeatedMessageHandler.transformFromParquet(mainMessageGroup);

        assertEquals(0, actualRows.length);
    }

    @Test
    public void shouldReturnEmptyRowArrayWhenTransformFromParquetIsCalledWithSimpleGroupWhichHasRepeatedSimpleGroupFieldAsMissing() {
        Descriptors.FieldDescriptor fieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        RepeatedMessageHandler repeatedMessageHandler = new RepeatedMessageHandler(fieldDescriptor);

        GroupType mainMessageSchema = buildMessage()
                .required(INT64).named("some_other_field")
                .named("MainMessage");
        SimpleGroup mainMessageGroup = new SimpleGroup(mainMessageSchema);

        Row[] actualRows = (Row[]) repeatedMessageHandler.transformFromParquet(mainMessageGroup);

        assertEquals(0, actualRows.length);
    }

    @Test
    public void shouldReturnEmptyArrayOfRowsIfNullPassedForTransformFromProtoUsingCache() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestFeedbackLogMessage.getDescriptor());
        Object[] values = (Object[]) new RepeatedMessageHandler(repeatedMessageFieldDescriptor).transformFromProtoUsingCache(null, fieldDescriptorCache);


        assertEquals(0, values.length);
    }

    @Test
    public void shouldReturnArrayOfRowsGivenAListForFieldDescriptorOfTypeRepeatedMessageOfAsDescriptorForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        TestFeedbackLogMessage logMessage = TestFeedbackLogMessage
                .newBuilder()
                .addReason(TestReason.newBuilder().setReasonId("reason1").setGroupId("group1").build())
                .addReason(TestReason.newBuilder().setReasonId("reason2").setGroupId("group2").build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestFeedbackLogMessage.getDescriptor(), logMessage.toByteArray());

        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestFeedbackLogMessage.getDescriptor().findFieldByName("reason");
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestFeedbackLogMessage.getDescriptor());

        Object[] values = (Object[]) new RepeatedMessageHandler(repeatedMessageFieldDescriptor).transformFromProtoUsingCache(dynamicMessage.getField(repeatedMessageFieldDescriptor), fieldDescriptorCache);

        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[0]).getArity());
        assertEquals(repeatedMessageFieldDescriptor.getMessageType().getFields().size(), ((Row) values[1]).getArity());
        assertEquals("reason1", ((Row) values[0]).getField(0));
        assertEquals("group1", ((Row) values[0]).getField(1));
        assertEquals("reason2", ((Row) values[1]).getField(0));
        assertEquals("group2", ((Row) values[1]).getField(1));
    }

}
