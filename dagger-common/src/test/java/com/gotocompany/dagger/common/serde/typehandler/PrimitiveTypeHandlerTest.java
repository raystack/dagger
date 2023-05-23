package com.gotocompany.dagger.common.serde.typehandler;

import com.google.protobuf.ByteString;
import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import com.gotocompany.dagger.consumer.TestMessageEnvelope;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.dagger.common.exceptions.serde.InvalidDataTypeException;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.*;

public class PrimitiveTypeHandlerTest {

    @Test
    public void shouldReturnTrueForAnyDataType() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(fieldDescriptor);

        assertTrue(primitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = primitiveTypeHandler.transformToProtoBuilder(builder, null);
        assertEquals("", returnedBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldSetFieldPassedInTheBuilder() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = primitiveTypeHandler.transformToProtoBuilder(builder, "123");
        assertEquals("123", returnedBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnIntegerValueForIntegerTypeFieldDescriptorIfIntegerIsPassedForPostProcessorTransform() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(fieldDescriptor);

        assertEquals(1, primitiveTypeHandler.transformFromPostProcessor(1));
    }

    @Test
    public void shouldReturnIntegerValueForIntegerTypeFieldDescriptorIfIntegerIsPassedAsStringForPostProcessorTransform() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(fieldDescriptor);

        assertEquals(1, primitiveTypeHandler.transformFromPostProcessor("1"));
    }

    @Test
    public void shouldReturnStringValueForStringTypeFieldDescriptorIfStringIsPassedForPostProcessorTransform() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(stringFieldDescriptor);

        assertEquals("123", primitiveTypeHandler.transformFromPostProcessor("123"));
    }

    @Test
    public void shouldReturnStringValueForStringTypeFieldDescriptorIfStringIsNotPassedForPostProcessorTransform() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(stringFieldDescriptor);

        assertEquals("123", primitiveTypeHandler.transformFromPostProcessor(123));
    }

    @Test
    public void shouldThrowInvalidDataTypeExceptionInCaseOfTypeMismatchForPostProcessorTransform() {
        Descriptors.FieldDescriptor floatFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("total_customer_discount");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(floatFieldDescriptor);

        InvalidDataTypeException exception = Assert.assertThrows(InvalidDataTypeException.class,
                () -> primitiveTypeHandler.transformFromPostProcessor("stringValue"));
        assertEquals("type mismatch of field: total_customer_discount, expecting FLOAT type, actual type class java.lang.String", exception.getMessage());
    }

    @Test
    public void shouldReturnSameValueForTransformFromProto() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(stringFieldDescriptor);

        assertEquals(123, primitiveTypeHandler.transformFromProto(123));
        assertEquals("123", primitiveTypeHandler.transformFromProto("123"));
    }

    @Test
    public void shouldReturnSameValueForTransformFromProtoUsingCache() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(stringFieldDescriptor);
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        assertEquals(123, primitiveTypeHandler.transformFromProtoUsingCache(123, fieldDescriptorCache));
        assertEquals("123", primitiveTypeHandler.transformFromProtoUsingCache("123", fieldDescriptorCache));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(stringFieldDescriptor);
        assertEquals(Types.STRING, primitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldConvertPrimitiveStringToJsonString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        Object value = new PrimitiveTypeHandler(fieldDescriptor).transformToJson("123");

        assertEquals("123", value);
    }

    @Test
    public void shouldReturnParsedValueForTransformFromParquet() {
        Descriptors.FieldDescriptor fieldDescriptor = TestMessageEnvelope.getDescriptor().findFieldByName("log_key");
        String testString = "test-string";
        ByteString expectedByteString = ByteString.copyFrom(testString.getBytes());
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(BINARY).named("log_key")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("log_key", Binary.fromConstantByteArray(expectedByteString.toByteArray()));
        PrimitiveTypeHandler primitiveTypeHandler = new PrimitiveTypeHandler(fieldDescriptor);

        ByteString actualByteString = (ByteString) primitiveTypeHandler.transformFromParquet(simpleGroup);

        assertEquals(expectedByteString, actualByteString);
    }
}
