package io.odpf.dagger.core.protohandler;

import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.core.exception.InvalidDataTypeException;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrimitiveProtoHandlerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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

        DynamicMessage.Builder returnedBuilder = primitiveProtoHandler.transformForKafka(builder, null);
        assertEquals("", returnedBuilder.getField(fieldDescriptor));
    }

    @Test
    public void shouldSetFieldPassedInTheBuilder() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = primitiveProtoHandler.transformForKafka(builder, "123");
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
        expectedException.expect(InvalidDataTypeException.class);
        expectedException.expectMessage("type mismatch of field: total_customer_discount, expecting FLOAT type, actual type class java.lang.String");

        Descriptors.FieldDescriptor floatFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("total_customer_discount");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(floatFieldDescriptor);

        primitiveProtoHandler.transformFromPostProcessor("stringValue");
    }

    @Test
    public void shouldReturnSameValueForTransformForKafka() {
        Descriptors.Descriptor descriptor = TestBookingLogMessage.getDescriptor();
        Descriptors.FieldDescriptor stringFieldDescriptor = descriptor.findFieldByName("order_number");
        PrimitiveProtoHandler primitiveProtoHandler = new PrimitiveProtoHandler(stringFieldDescriptor);

        assertEquals(123, primitiveProtoHandler.transformFromKafka(123));
        assertEquals("123", primitiveProtoHandler.transformFromKafka("123"));
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

        Assert.assertEquals("123", value);
    }
}
