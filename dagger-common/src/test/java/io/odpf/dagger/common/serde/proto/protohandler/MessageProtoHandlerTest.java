package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestPaymentOptionMetadata;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageProtoHandler messsageProtoHandler = new MessageProtoHandler(messageFieldDescriptor);

        assertTrue(messsageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanMessageTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MessageProtoHandler messsageProtoHandler = new MessageProtoHandler(otherFieldDescriptor);

        assertFalse(messsageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MessageProtoHandler messsageProtoHandler = new MessageProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals(builder, messsageProtoHandler.transformForKafka(builder, 123));
        assertEquals("", messsageProtoHandler.transformForKafka(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingFieldIfNullPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        MessageProtoHandler messsageProtoHandler = new MessageProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder outputBuilder = messsageProtoHandler.transformForKafka(builder, null);
        assertEquals(builder, outputBuilder);
        assertEquals("", outputBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForMessageFieldTypeDescriptorIfAllFieldsPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageProtoHandler messageProtoHandler = new MessageProtoHandler(messageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageFieldDescriptor.getContainingType());

        Row inputRow = new Row(2);
        inputRow.setField(0, "test1");
        inputRow.setField(1, "test2");
        DynamicMessage.Builder returnedBuilder = messageProtoHandler.transformForKafka(builder, inputRow);

        TestPaymentOptionMetadata returnedValue = TestPaymentOptionMetadata.parseFrom(((DynamicMessage) returnedBuilder.getField(messageFieldDescriptor)).toByteArray());

        assertEquals("test1", returnedValue.getMaskedCard());
        assertEquals("test2", returnedValue.getNetwork());

    }

    @Test
    public void shouldSetTheFieldsPassedInTheBuilderForMessageFieldTypeDescriptorIfAllFieldsAreNotPassed() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor messageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("payment_option_metadata");
        MessageProtoHandler messageProtoHandler = new MessageProtoHandler(messageFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageFieldDescriptor.getContainingType());

        Row inputRow = new Row(1);
        inputRow.setField(0, "test1");
        DynamicMessage.Builder returnedBuilder = messageProtoHandler.transformForKafka(builder, inputRow);

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

        Row value = (Row) ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor(inputValues);

        assertEquals("test1", value.getField(0));
        assertEquals("test2", value.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueAreNotPassedForTransformForPostProcessor() {
        HashMap<String, String> inputValues = new HashMap<>();
        inputValues.put("masked_card", "test1");

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor(inputValues);

        assertEquals("test1", value.getField(0));
        assertEquals(null, value.getField(1));
    }

    @Test
    public void shouldReturnEmptyRowIfNullPassedForTransformForPostProcessor() {
        HashMap<String, String> inputValues = new HashMap<>();
        inputValues.put("masked_card", "test1");

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor(null);

        assertEquals(2, value.getArity());
        assertEquals(null, value.getField(0));
        assertEquals(null, value.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueArePassedForTransformForKafka() throws InvalidProtocolBufferException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .setPaymentOptionMetadata(TestPaymentOptionMetadata.newBuilder().setMaskedCard("test1").setNetwork("test2").build())
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) new MessageProtoHandler(fieldDescriptor).transformFromKafka(dynamicMessage.getField(fieldDescriptor));

        assertEquals("test1", value.getField(0));
        assertEquals("test2", value.getField(1));
    }

    @Test
    public void shouldReturnRowGivenAMapForFieldDescriptorOfTypeMessageIfAllValueAreNotPassedForTransformForKafka() throws InvalidProtocolBufferException {
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .setPaymentOptionMetadata(TestPaymentOptionMetadata.newBuilder().setMaskedCard("test1").build())
                .build();

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), bookingLogMessage.toByteArray());

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");

        Row value = (Row) new MessageProtoHandler(fieldDescriptor).transformFromKafka(dynamicMessage.getField(fieldDescriptor));

        assertEquals("test1", value.getField(0));
        assertEquals("", value.getField(1));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("payment_option_metadata");
        TypeInformation actualTypeInformation = new MessageProtoHandler(fieldDescriptor).getTypeInformation();
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

        Object value = new MessageProtoHandler(fieldDescriptor).transformToJson(inputRow);

        assertEquals("{\"masked_card\":\"test1\",\"network\":\"test2\"}", String.valueOf(value));
    }
}
