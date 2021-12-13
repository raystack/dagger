package io.odpf.dagger.common.serde.proto.protohandler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestRepeatedEnumMessage;
import io.odpf.dagger.consumer.TestServiceType;
import io.odpf.dagger.common.exceptions.serde.EnumFieldNotFoundException;
import org.apache.flink.api.common.typeinfo.Types;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class EnumProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(enumFieldDescriptor);

        assertTrue(enumProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfRepeatedEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(repeatedEnumFieldDescriptor);

        assertFalse(enumProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanEnumTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(otherFieldDescriptor);

        assertFalse(enumProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals("", enumProtoHandler.transformForKafka(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingIfNullPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals("", enumProtoHandler.transformForKafka(builder, null).getField(fieldDescriptor));
    }

    @Test
    public void shouldSetTheFieldPassedInTheBuilderForEnumFieldTypeDescriptor() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(enumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(enumFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = enumProtoHandler.transformForKafka(builder, "GO_RIDE");
        assertEquals(TestServiceType.Enum.GO_RIDE.getValueDescriptor(), returnedBuilder.getField(enumFieldDescriptor));
    }

    @Test
    public void shouldThrowExceptionIfFieldNotFoundInGivenEnumFieldTypeDescriptor() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(enumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(enumFieldDescriptor.getContainingType());

        EnumFieldNotFoundException exception = Assert.assertThrows(EnumFieldNotFoundException.class, () -> enumProtoHandler.transformForKafka(builder, "test"));
        assertEquals("field: test not found in io.odpf.dagger.consumer.TestBookingLogMessage.service_type", exception.getMessage());
    }

    @Test
    public void shouldReturnEnumStringGivenEnumStringForFieldDescriptorOfTypeEnum() {
        String inputField = "DRIVER_FOUND";

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor(inputField);

        assertEquals("DRIVER_FOUND", value);
    }

    @Test
    public void shouldReturnDefaultEnumStringIfNotFoundForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor(null);

        assertEquals("UNKNOWN", value);
    }

    @Test
    public void shouldReturnDefaultEnumStringIfInputIsAEnumPositionAndNotInTheProtoDefinitionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor(-1);

        assertEquals("UNKNOWN", value);
    }

    @Test
    public void shouldReturnDefaultEnumStringIfInputIsAStringAndNotInTheProtoDefinitionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor("dummy");

        assertEquals("UNKNOWN", value);
    }

    @Test
    public void shouldReturnDefaultEnumStringIfInputIsNullForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor(null);

        assertEquals("UNKNOWN", value);
    }

    @Test
    public void shouldReturnEnumStringGivenEnumPositionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = ProtoHandlerFactory.getProtoHandler(fieldDescriptor).transformFromPostProcessor(2);

        assertEquals("DRIVER_FOUND", value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(fieldDescriptor);
        assertEquals(Types.STRING, enumProtoHandler.getTypeInformation());
    }

    @Test
    public void shouldTransformValueForKafka() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(fieldDescriptor);
        assertEquals("DRIVER_FOUND", enumProtoHandler.transformFromKafka("DRIVER_FOUND"));
    }

    @Test
    public void shouldConvertEnumToJsonString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        Object value = new EnumProtoHandler(fieldDescriptor).transformToJson("DRIVER_FOUND");

        assertEquals("DRIVER_FOUND", value);
    }
}
