package io.odpf.dagger.common.serde.typehandler.complex;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.common.serde.typehandler.ProtoHandlerFactory;
import io.odpf.dagger.consumer.*;
import io.odpf.dagger.common.exceptions.serde.EnumFieldNotFoundException;
import org.apache.flink.api.common.typeinfo.Types;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.*;

public class EnumTypeHandlerTest {

    @Test
    public void shouldReturnTrueIfEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(enumFieldDescriptor);

        assertTrue(enumTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfRepeatedEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(repeatedEnumFieldDescriptor);

        assertFalse(enumTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanEnumTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(otherFieldDescriptor);

        assertFalse(enumTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals("", enumTypeHandler.transformToProtoBuilder(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingIfNullPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals("", enumTypeHandler.transformToProtoBuilder(builder, null).getField(fieldDescriptor));
    }

    @Test
    public void shouldSetTheFieldPassedInTheBuilderForEnumFieldTypeDescriptor() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(enumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(enumFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = enumTypeHandler.transformToProtoBuilder(builder, "GO_RIDE");
        assertEquals(TestServiceType.Enum.GO_RIDE.getValueDescriptor(), returnedBuilder.getField(enumFieldDescriptor));
    }

    @Test
    public void shouldThrowExceptionIfFieldNotFoundInGivenEnumFieldTypeDescriptor() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(enumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(enumFieldDescriptor.getContainingType());

        EnumFieldNotFoundException exception = Assert.assertThrows(EnumFieldNotFoundException.class, () -> enumTypeHandler.transformToProtoBuilder(builder, "test"));
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
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(fieldDescriptor);
        assertEquals(Types.STRING, enumTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldTransformValueForKafka() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(fieldDescriptor);
        assertEquals("DRIVER_FOUND", enumTypeHandler.transformFromProto("DRIVER_FOUND"));
    }

    @Test
    public void shouldConvertEnumToJsonString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        Object value = new EnumTypeHandler(fieldDescriptor).transformToJson("DRIVER_FOUND");

        assertEquals("DRIVER_FOUND", value);
    }

    @Test
    public void shouldReturnStringEnumValueWhenSimpleGroupIsPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        String expectedEnum = fieldDescriptor.getEnumType().getValues().get(1).getName();
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("status")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("status", expectedEnum);
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(fieldDescriptor);

        Object actualEnum = enumTypeHandler.transformFromParquet(simpleGroup);

        assertEquals(expectedEnum, actualEnum);
    }

    @Test
    public void shouldReturnDefaultEnumStringWhenNullIsPassedToTransformFromParquet() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(fieldDescriptor);

        Object actualEnumValue = enumTypeHandler.transformFromParquet(null);

        assertEquals("UNKNOWN", actualEnumValue);
    }

    @Test
    public void shouldReturnDefaultEnumStringWhenEnumValueInsideSimpleGroupIsNotPresentInProtoDefinition() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("status")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("status", "NON_EXISTENT_ENUM");
        EnumTypeHandler enumTypeHandler = new EnumTypeHandler(fieldDescriptor);

        Object actualEnum = enumTypeHandler.transformFromParquet(simpleGroup);

        assertEquals("UNKNOWN", actualEnum);
    }
}
