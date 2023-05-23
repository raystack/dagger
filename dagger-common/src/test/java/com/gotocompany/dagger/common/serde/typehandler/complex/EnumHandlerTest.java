package com.gotocompany.dagger.common.serde.typehandler.complex;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandlerFactory;
import com.gotocompany.dagger.consumer.*;
import com.gotocompany.dagger.common.exceptions.serde.EnumFieldNotFoundException;
import org.apache.flink.api.common.typeinfo.Types;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.*;

public class EnumHandlerTest {

    @Test
    public void shouldReturnTrueIfEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumHandler enumHandler = new EnumHandler(enumFieldDescriptor);

        assertTrue(enumHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfRepeatedEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        EnumHandler enumHandler = new EnumHandler(repeatedEnumFieldDescriptor);

        assertFalse(enumHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanEnumTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumHandler enumHandler = new EnumHandler(otherFieldDescriptor);

        assertFalse(enumHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumHandler enumHandler = new EnumHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals("", enumHandler.transformToProtoBuilder(builder, 123).getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingIfNullPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumHandler enumHandler = new EnumHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals("", enumHandler.transformToProtoBuilder(builder, null).getField(fieldDescriptor));
    }

    @Test
    public void shouldSetTheFieldPassedInTheBuilderForEnumFieldTypeDescriptor() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumHandler enumHandler = new EnumHandler(enumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(enumFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = enumHandler.transformToProtoBuilder(builder, "GO_RIDE");
        assertEquals(TestServiceType.Enum.GO_RIDE.getValueDescriptor(), returnedBuilder.getField(enumFieldDescriptor));
    }

    @Test
    public void shouldThrowExceptionIfFieldNotFoundInGivenEnumFieldTypeDescriptor() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumHandler enumHandler = new EnumHandler(enumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(enumFieldDescriptor.getContainingType());

        EnumFieldNotFoundException exception = Assert.assertThrows(EnumFieldNotFoundException.class, () -> enumHandler.transformToProtoBuilder(builder, "test"));
        assertEquals("field: test not found in com.gotocompany.dagger.consumer.TestBookingLogMessage.service_type", exception.getMessage());
    }

    @Test
    public void shouldReturnEnumStringGivenEnumStringForFieldDescriptorOfTypeEnum() {
        String inputField = "DRIVER_FOUND";

        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor(inputField);

        assertEquals("DRIVER_FOUND", value);
    }

    @Test
    public void shouldReturnDefaultEnumStringIfNotFoundForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor(null);

        assertEquals("UNKNOWN", value);
    }

    @Test
    public void shouldReturnDefaultEnumStringIfInputIsAEnumPositionAndNotInTheProtoDefinitionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor(-1);

        assertEquals("UNKNOWN", value);
    }

    @Test
    public void shouldReturnDefaultEnumStringIfInputIsAStringAndNotInTheProtoDefinitionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor("dummy");

        assertEquals("UNKNOWN", value);
    }

    @Test
    public void shouldReturnDefaultEnumStringIfInputIsNullForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor(null);

        assertEquals("UNKNOWN", value);
    }

    @Test
    public void shouldReturnEnumStringGivenEnumPositionForFieldDescriptorOfTypeEnum() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");

        Object value = TypeHandlerFactory.getTypeHandler(fieldDescriptor).transformFromPostProcessor(2);

        assertEquals("DRIVER_FOUND", value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");
        EnumHandler enumHandler = new EnumHandler(fieldDescriptor);
        assertEquals(Types.STRING, enumHandler.getTypeInformation());
    }

    @Test
    public void shouldTransformValueFromProto() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");
        EnumHandler enumHandler = new EnumHandler(fieldDescriptor);
        assertEquals("DRIVER_FOUND", enumHandler.transformFromProto("DRIVER_FOUND"));
    }

    @Test
    public void shouldTransformValueFromProtoUsingCache() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("status");
        EnumHandler enumHandler = new EnumHandler(fieldDescriptor);
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        assertEquals("DRIVER_FOUND", enumHandler.transformFromProtoUsingCache("DRIVER_FOUND", fieldDescriptorCache));
    }

    @Test
    public void shouldConvertEnumToJsonString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        Object value = new EnumHandler(fieldDescriptor).transformToJson("DRIVER_FOUND");

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
        EnumHandler enumHandler = new EnumHandler(fieldDescriptor);

        Object actualEnum = enumHandler.transformFromParquet(simpleGroup);

        assertEquals(expectedEnum, actualEnum);
    }

    @Test
    public void shouldReturnDefaultEnumStringWhenNullIsPassedToTransformFromParquet() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        EnumHandler enumHandler = new EnumHandler(fieldDescriptor);

        Object actualEnumValue = enumHandler.transformFromParquet(null);

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
        EnumHandler enumHandler = new EnumHandler(fieldDescriptor);

        Object actualEnum = enumHandler.transformFromParquet(simpleGroup);

        assertEquals("UNKNOWN", actualEnum);
    }
}
