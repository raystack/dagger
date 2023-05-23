package com.gotocompany.dagger.common.serde.typehandler.repeated;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.dagger.common.exceptions.serde.DataTypeNotSupportedException;
import com.gotocompany.dagger.common.exceptions.serde.InvalidDataTypeException;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestRepeatedEnumMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.*;

public class RepeatedPrimitiveHandlerTest {

    @Test
    public void shouldReturnTrueIfRepeatedPrimitiveFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);

        assertTrue(repeatedPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfRepeatedMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("routes");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedMessageFieldDescriptor);

        assertFalse(repeatedPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfRepeatedEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedEnumFieldDescriptor);

        assertFalse(repeatedPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(otherFieldDescriptor);

        assertFalse(repeatedPrimitiveHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfCannotHandle() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(otherFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(otherFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveHandler.transformToProtoBuilder(builder, "123");
        assertEquals("", returnedBuilder.getField(otherFieldDescriptor));
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveHandler.transformToProtoBuilder(builder, null);
        List<Object> outputValues = (List<Object>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldSetEmptyListInBuilderIfEmptyListIfPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveHandler.transformToProtoBuilder(builder, inputValues);
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldSetFieldPassedInTheBuilderAsAList() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveHandler.transformToProtoBuilder(builder, inputValues);
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(asList("test1", "test2"), outputValues);
    }

    @Test
    public void shouldSetFieldPassedInTheBuilderAsArray() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveHandler.transformToProtoBuilder(builder, inputValues.toArray());
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(asList("test1", "test2"), outputValues);
    }

    @Test
    public void shouldReturnArrayOfObjectsWithTypeSameAsFieldDescriptorForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();
        inputValues.add(1);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveHandler.transformFromPostProcessor(inputValues);

        assertEquals(String.class, outputValues.get(0).getClass());
    }

    @Test
    public void shouldReturnEmptyArrayOfObjectsIfEmptyListPassedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();

        List<Object> outputValues = (List<Object>) repeatedPrimitiveHandler.transformFromPostProcessor(inputValues);

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnEmptyArrayOfObjectsIfNullPassedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveHandler.transformFromPostProcessor(null);

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnAllFieldsInAListOfObjectsIfMultipleFieldsPassedWithSameTypeAsFieldDescriptorForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();
        inputValues.add(1);
        inputValues.add(2);
        inputValues.add(3);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveHandler.transformFromPostProcessor(inputValues);

        assertEquals(asList("1", "2", "3"), outputValues);
    }

    @Test
    public void shouldThrowExceptionIfFieldDesciptorTypeNotSupportedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("routes");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test");
        DataTypeNotSupportedException exception = Assert.assertThrows(DataTypeNotSupportedException.class,
                () -> repeatedPrimitiveHandler.transformFromPostProcessor(inputValues));
        assertEquals("Data type MESSAGE not supported in primitive type handlers", exception.getMessage());
    }

    @Test
    public void shouldThrowInvalidDataTypeExceptionInCaseOfTypeMismatchForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFloatFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("int_array_field");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFloatFieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test");
        InvalidDataTypeException exception = Assert.assertThrows(InvalidDataTypeException.class,
                () -> repeatedPrimitiveHandler.transformFromPostProcessor(inputValues));
        assertEquals("type mismatch of field: int_array_field, expecting INT32 type, actual type class java.lang.String", exception.getMessage());
    }

    @Test
    public void shouldReturnAllFieldsInAListOfObjectsIfMultipleFieldsPassedWithSameTypeAsFieldDescriptorForTransformFromProto() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);

        TestBookingLogMessage goLifeBookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .addMetaArray("1")
                .addMetaArray("2")
                .addMetaArray("3")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), goLifeBookingLogMessage.toByteArray());

        String[] outputValues = (String[]) repeatedPrimitiveHandler.transformFromProto(dynamicMessage.getField(repeatedFieldDescriptor));
        assertArrayEquals(new String[]{"1", "2", "3"}, outputValues);
    }

    @Test
    public void shouldThrowUnsupportedDataTypeExceptionInCaseOfInCaseOfEnumForTransformFromProto() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(fieldDescriptor);
        DataTypeNotSupportedException exception = Assert.assertThrows(DataTypeNotSupportedException.class,
                () -> repeatedPrimitiveHandler.transformFromProto("CREATED"));
        assertEquals("Data type ENUM not supported in primitive type handlers", exception.getMessage());
    }

    @Test
    public void shouldReturnAllFieldsInAListOfObjectsIfMultipleFieldsPassedWithSameTypeAsFieldDescriptorForTransformFromProtoUsingCache() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);

        TestBookingLogMessage goLifeBookingLogMessage = TestBookingLogMessage
                .newBuilder()
                .addMetaArray("1")
                .addMetaArray("2")
                .addMetaArray("3")
                .build();
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestBookingLogMessage.getDescriptor(), goLifeBookingLogMessage.toByteArray());

        String[] outputValues = (String[]) repeatedPrimitiveHandler.transformFromProtoUsingCache(dynamicMessage.getField(repeatedFieldDescriptor), fieldDescriptorCache);
        assertArrayEquals(new String[]{"1", "2", "3"}, outputValues);
    }

    @Test
    public void shouldThrowUnsupportedDataTypeExceptionInCaseOfInCaseOfEnumForTransformFromProtoUsingCache() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("status");
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(fieldDescriptor);
        DataTypeNotSupportedException exception = Assert.assertThrows(DataTypeNotSupportedException.class,
                () -> repeatedPrimitiveHandler.transformFromProtoUsingCache("CREATED", fieldDescriptorCache));
        assertEquals("Data type ENUM not supported in primitive type handlers", exception.getMessage());
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(repeatedFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedPrimitiveHandler.getTypeInformation();
        TypeInformation<String[]> expectedTypeInformation = ObjectArrayTypeInfo.getInfoFor(Types.STRING);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldConvertRepeatedRowDataToJsonString() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        Object value = new RepeatedPrimitiveHandler(repeatedFieldDescriptor).transformToJson(inputValues);
        assertEquals("[\"test1\",\"test2\"]", String.valueOf(value));
    }

    @Test
    public void shouldReturnArrayOfPrimitiveValuesWhenTransformFromParquetIsCalledWithSimpleGroupContainingRepeatedPrimitive() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("meta_array");

        RepeatedPrimitiveHandler repeatedPrimitiveHandler = new RepeatedPrimitiveHandler(fieldDescriptor);

        GroupType parquetSchema = buildMessage()
                .repeated(BINARY).named("meta_array")
                .named("TestBookingLogMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        simpleGroup.add("meta_array", "Hello World");
        simpleGroup.add("meta_array", "Welcome");

        String[] actualValue = (String[]) repeatedPrimitiveHandler.transformFromParquet(simpleGroup);

        assertArrayEquals(new String[]{"Hello World", "Welcome"}, actualValue);
    }
}
