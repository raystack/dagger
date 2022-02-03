package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.common.exceptions.serde.DataTypeNotSupportedException;
import io.odpf.dagger.common.exceptions.serde.InvalidDataTypeException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestRepeatedEnumMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepeatedPrimitiveProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfRepeatedPrimitiveFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        assertTrue(repeatedPrimitiveProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfRepeatedMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("routes");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedMessageFieldDescriptor);

        assertFalse(repeatedPrimitiveProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfRepeatedEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedEnumFieldDescriptor);

        assertFalse(repeatedPrimitiveProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(otherFieldDescriptor);

        assertFalse(repeatedPrimitiveProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfCannotHandle() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(otherFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(otherFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, "123");
        assertEquals("", returnedBuilder.getField(otherFieldDescriptor));
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, null);
        List<Object> outputValues = (List<Object>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldSetEmptyListInBuilderIfEmptyListIfPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, inputValues);
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldSetFieldPassedInTheBuilderAsAList() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, inputValues);
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(asList("test1", "test2"), outputValues);
    }

    @Test
    public void shouldSetFieldPassedInTheBuilderAsArray() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, inputValues.toArray());
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(asList("test1", "test2"), outputValues);
    }

    @Test
    public void shouldReturnArrayOfObjectsWithTypeSameAsFieldDescriptorForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();
        inputValues.add(1);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues);

        assertEquals(String.class, outputValues.get(0).getClass());
    }

    @Test
    public void shouldReturnEmptyArrayOfObjectsIfEmptyListPassedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();

        List<Object> outputValues = (List<Object>) repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues);

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnEmptyArrayOfObjectsIfNullPassedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveProtoHandler.transformFromPostProcessor(null);

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnAllFieldsInAListOfObjectsIfMultipleFieldsPassedWithSameTypeAsFieldDescriptorForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();
        inputValues.add(1);
        inputValues.add(2);
        inputValues.add(3);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues);

        assertEquals(asList("1", "2", "3"), outputValues);
    }

    @Test
    public void shouldThrowExceptionIfFieldDesciptorTypeNotSupportedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("routes");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test");
        DataTypeNotSupportedException exception = Assert.assertThrows(DataTypeNotSupportedException.class,
                () -> repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues));
        assertEquals("Data type MESSAGE not supported in primitive type handlers", exception.getMessage());
    }

    @Test
    public void shouldThrowInvalidDataTypeExceptionInCaseOfTypeMismatchForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFloatFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("int_array_field");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFloatFieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test");
        InvalidDataTypeException exception = Assert.assertThrows(InvalidDataTypeException.class,
                () -> repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues));
        assertEquals("type mismatch of field: int_array_field, expecting INT32 type, actual type class java.lang.String", exception.getMessage());
    }

    @Test
    public void shouldReturnAllFieldsInAListOfObjectsIfMultipleFieldsPassedWithSameTypeAsFieldDescriptorForKafkaTransform() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        TestBookingLogMessage goLifeBookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .addMetaArray("1")
                .addMetaArray("2")
                .addMetaArray("3")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), goLifeBookingLogMessage.toByteArray());

        String[] outputValues = (String[]) repeatedPrimitiveProtoHandler.transformFromKafka(dynamicMessage.getField(repeatedFieldDescriptor));
        assertArrayEquals(new String[]{"1", "2", "3"}, outputValues);
    }


    @Test
    public void shouldThrowUnsupportedDataTypeExceptionInCaseOfInCaseOfEnumForKafkaTransform() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(fieldDescriptor);
        DataTypeNotSupportedException exception = Assert.assertThrows(DataTypeNotSupportedException.class,
                () -> repeatedPrimitiveProtoHandler.transformFromKafka("CREATED"));
        assertEquals("Data type ENUM not supported in primitive type handlers", exception.getMessage());
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedPrimitiveProtoHandler.getTypeInformation();
        TypeInformation<String[]> expectedTypeInformation = ObjectArrayTypeInfo.getInfoFor(Types.STRING);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldConvertRepeatedRowDataToJsonString() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        Object value = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor).transformToJson(inputValues);
        assertEquals("[\"test1\",\"test2\"]", String.valueOf(value));
    }
}
