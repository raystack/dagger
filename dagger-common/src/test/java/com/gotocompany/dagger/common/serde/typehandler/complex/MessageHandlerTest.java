package com.gotocompany.dagger.common.serde.typehandler.complex;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandlerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestPaymentOptionMetadata;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.util.HashMap;

import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.requiredGroup;
import static org.junit.Assert.*;

public class MessageHandlerTest {

    @Test
    public void shouldReturnTrueIfMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageHandler messageHandler = new MessageHandler(messageFieldDescriptor);

        assertTrue(messageHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanMessageTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MessageHandler messageHandler = new MessageHandler(otherFieldDescriptor);

        assertFalse(messageHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MessageHandler messageHandler = new MessageHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals(builder, messageHandler.transformToProtoBuilder(builder, 123));
        assertEquals("", messageHandler.transformToProtoBuilder(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfNullPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MessageHandler messageHandler = new MessageHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder outputBuilder = messageHandler.transformToProtoBuilder(builder, null);
        assertEquals(builder, outputBuilder);
        assertEquals("", outputBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForMessageFieldTypeDescriptorIfAllFieldsPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageHandler messageHandler = new MessageHandler(messageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageFieldDescriptor.getContainingType());

        Row inputRow = new Row(2);
        inputRow.setField(0, "test1");
        inputRow.setField(1, "test2");
        DynamicMessage.Builder returnedBuilder = messageHandler.transformToProtoBuilder(builder, inputRow);

        TestPaymentOptionMetadata returnedValue = TestPaymentOptionMetadata.parseFrom(((DynamicMessage) returnedBuilder.getField(messageFieldDescriptor)).toByteArray());

        assertEquals("test1", returnedValue.getMaskedCard());
        assertEquals("test2", returnedValue.getNetwork());

    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForMessageFieldTypeDescriptorIfAllFieldsAreNotPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageHandler messageHandler = new MessageHandler(messageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageFieldDescriptor.getContainingType());

        Row inputRow = new Row(1);
        inputRow.setField(0, "test1");
        DynamicMessage.Builder returnedBuilder = messageHandler.transformToProtoBuilder(builder, inputRow);

        TestPaymentOptionMetadata returnedValue = TestPaymentOptionMetadata.parseFrom(((DynamicMessage) returnedBuilder.getField(messageFieldDescriptor)).toByteArray());

        assertEquals("test1", returnedValue.getMaskedCard());
        assertEquals("", returnedValue.getNetwork());

    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueArePassedForTransformForPostProcessor() {
        HashMap<String, String> inputValues = new HashMap<>();
        inputValues.put("masked_card", "test1");
        inputValues.put("network", "test2");

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor(inputValues);

        assertEquals("test1", value.getField(0));
        assertEquals("test2", value.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueAreNotPassedForTransformForPostProcessor() {
        HashMap<String, String> inputValues = new HashMap<>();
        inputValues.put("masked_card", "test1");

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor(inputValues);

        assertEquals("test1", value.getField(0));
        assertEquals(null, value.getField(1));
    }

    @Test
    public void shouldReturnEmptyRowIfNullPassedForTransformForPostProcessor() {
        HashMap<String, String> inputValues = new HashMap<>();
        inputValues.put("masked_card", "test1");

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor(null);

        assertEquals(2, value.getArity());
        assertEquals(null, value.getField(0));
        assertEquals(null, value.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueArePassedForTransformFromProto() throws InvalidProtocolBufferException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .setPaymentOptionMetadata(TestPaymentOptionMetadata.newBuilder().setMaskedCard("test1").setNetwork("test2").build())
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) new MessageHandler(fieldDescriptor).transformFromProto(dynamicMessage.getField(fieldDescriptor));

        assertEquals("test1", value.getField(0));
        assertEquals("test2", value.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueAreNotPassedForTransformFromProto() throws InvalidProtocolBufferException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .setPaymentOptionMetadata(TestPaymentOptionMetadata.newBuilder().setMaskedCard("test1").build())
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) new MessageHandler(fieldDescriptor).transformFromProto(dynamicMessage.getField(fieldDescriptor));

        assertEquals("test1", value.getField(0));
        assertEquals("", value.getField(1));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");
        TypeInformation actualTypeInformation = new MessageHandler(fieldDescriptor).getTypeInformation();
        TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(new String[]{"masked_card", "network"}, Types.STRING, Types.STRING);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldConvertComplexRowDataToJsonString() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row inputRow = new Row(2);
        inputRow.setField(0, "test1");
        inputRow.setField(1, "test2");

        Object value = new MessageHandler(fieldDescriptor).transformToJson(inputRow);

        assertEquals("{\"masked_card\":\"test1\",\"network\":\"test2\"}", String.valueOf(value));
    }

    @Test
    public void shouldReturnRowContainingAllFieldsWhenTransformFromParquetIsCalledWithANestedSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageHandler messageHandler = new MessageHandler(fieldDescriptor);

        GroupType nestedGroupSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("masked_card")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("network")
                .named("payment_option_metadata");
        SimpleGroup nestedGroup = new SimpleGroup(nestedGroupSchema);
        nestedGroup.add("masked_card", "4567XXXX1234");
        nestedGroup.add("network", "4G");

        GroupType mainMessageSchema = buildMessage().addField(nestedGroupSchema).named("MainMessage");
        SimpleGroup mainMessageGroup = new SimpleGroup(mainMessageSchema);
        mainMessageGroup.add("payment_option_metadata", nestedGroup);

        Row row = (Row) messageHandler.transformFromParquet(mainMessageGroup);

        assertEquals(2, row.getArity());
        assertEquals("4567XXXX1234", row.getField(0));
        assertEquals("4G", row.getField(1));
    }

    @Test
    public void shouldReturnRowContainingDefaultValuesForFieldsWhenTransformFromParquetIsCalledWithUninitializedNestedSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageHandler messageHandler = new MessageHandler(fieldDescriptor);

        GroupType nestedGroupSchema = requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("masked_card")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("network")
                .named("payment_option_metadata");

        GroupType mainMessageSchema = buildMessage().addField(nestedGroupSchema).named("MainMessage");
        SimpleGroup mainMessageGroup = new SimpleGroup(mainMessageSchema);

        Row row = (Row) messageHandler.transformFromParquet(mainMessageGroup);

        assertEquals(2, row.getArity());
        assertEquals("", row.getField(0));
        assertEquals("", row.getField(1));
    }

    @Test
    public void shouldReturnRowContainingDefaultValuesForFieldsWhenTransformFromParquetIsCalledWithMissingNestedSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageHandler messageHandler = new MessageHandler(fieldDescriptor);

        GroupType mainMessageSchema = buildMessage().named("MainMessage");
        SimpleGroup mainMessageGroup = new SimpleGroup(mainMessageSchema);

        Row row = (Row) messageHandler.transformFromParquet(mainMessageGroup);

        assertEquals(2, row.getArity());
        assertEquals("", row.getField(0));
        assertEquals("", row.getField(1));
    }

    @Test
    public void shouldReturnRowContainingDefaultValuesForFieldsWhenTransformFromParquetIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageHandler messageHandler = new MessageHandler(fieldDescriptor);

        Row row = (Row) messageHandler.transformFromParquet(null);

        assertEquals(2, row.getArity());
        assertEquals("", row.getField(0));
        assertEquals("", row.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueArePassedForTransformFromProtoMap() throws InvalidProtocolBufferException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .setPaymentOptionMetadata(TestPaymentOptionMetadata.newBuilder().setMaskedCard("test1").setNetwork("test2").build())
                .build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestPaymentOptionMetadata.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) new MessageHandler(fieldDescriptor).transformFromProtoUsingCache(dynamicMessage.getField(fieldDescriptor), fieldDescriptorCache);

        assertEquals("test1", value.getField(0));
        assertEquals("test2", value.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueAreNotPassedForTransformFromProtoMap() throws InvalidProtocolBufferException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .setPaymentOptionMetadata(TestPaymentOptionMetadata.newBuilder().setMaskedCard("test1").build())
                .build();

        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestPaymentOptionMetadata.getDescriptor());
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) new MessageHandler(fieldDescriptor).transformFromProtoUsingCache(dynamicMessage.getField(fieldDescriptor), fieldDescriptorCache);

        assertEquals("test1", value.getField(0));
        assertEquals("", value.getField(1));
    }
}
