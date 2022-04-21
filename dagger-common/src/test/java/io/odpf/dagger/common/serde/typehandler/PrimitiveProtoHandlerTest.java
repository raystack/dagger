package io.odpf.dagger.common.serde.typehandler;

import com.google.protobuf.ByteString;
import io.odpf.dagger.common.serde.typehandler.PrimitiveProtoHandler;
import io.odpf.dagger.consumer.TestMessageEnvelope;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.common.exceptions.serde.InvalidDataTypeException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.*;

public class PrimitiveProtoHandlerTest {

    @Test
    public void shouldReturnTrueForAnyDataType() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);

        assertTrue(primitiveProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = primitiveProtoHandler.transformToProtoBuilder(builder, null);
        assertEquals("", returnedBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldSetFieldPassedInTheBuilder() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = primitiveProtoHandler.transformToProtoBuilder(builder, "123");
        assertEquals("123", returnedBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldReturnIntegerValueForIntegerTypeFieldDescriptorIfIntegerIsPassedForPostProcessorTransform() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);

        assertEquals(1, primitiveProtoHandler.transformFromPostProcessor(1));
    }

    @Test
    public void shouldReturnIntegerValueForIntegerTypeFieldDescriptorIfIntegerIsPassedAsStringForPostProcessorTransform() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("cancel_reason_id");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);

        assertEquals(1, primitiveProtoHandler.transformFromPostProcessor("1"));
    }

    @Test
    public void shouldReturnStringValueForStringTypeFieldDescriptorIfStringIsPassedForPostProcessorTransform() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(stringFieldDescriptor);

        assertEquals("123", primitiveProtoHandler.transformFromPostProcessor("123"));
    }

    @Test
    public void shouldReturnStringValueForStringTypeFieldDescriptorIfStringIsNotPassedForPostProcessorTransform() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(stringFieldDescriptor);

        assertEquals("123", primitiveProtoHandler.transformFromPostProcessor(123));
    }

    @Test
    public void shouldThrowInvalidDataTypeExceptionInCaseOfTypeMismatchForPostProcessorTransform() {
        Descriptors.FieldDescriptor floatFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("total_customer_discount");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(floatFieldDescriptor);

        InvalidDataTypeException exception = Assert.assertThrows(InvalidDataTypeException.class,
                () -> primitiveProtoHandler.transformFromPostProcessor("stringValue"));
        assertEquals("type mismatch of field: total_customer_discount, expecting FLOAT type, actual type class java.lang.String", exception.getMessage());
    }

    @Test
    public void shouldReturnSameValueForTransformFromKafka() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(stringFieldDescriptor);

        assertEquals(123, primitiveProtoHandler.transformFromProto(123));
        assertEquals("123", primitiveProtoHandler.transformFromProto("123"));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(stringFieldDescriptor);
        assertEquals(Types.STRING, primitiveProtoHandler.getTypeInformation());
    }

    @Test
    public void shouldConvertPrimitiveStringToJsonString() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        Object value = new PrimitiveProtoHandler(fieldDescriptor).transformToJson("123");

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
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);

        ByteString actualByteString = (ByteString) primitiveProtoHandler.transformFromParquet(simpleGroup);

        assertEquals(expectedByteString, actualByteString);
    }
}
