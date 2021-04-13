package io.odpf.dagger.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import io.odpf.dagger.exception.DataTypeNotSupportedException;
import io.odpf.dagger.exception.InvalidDataTypeException;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.booking.GoLifeBookingLogMessage;
import com.gojek.esb.consumer.TestRepeatedEnumMessage;
import com.gojek.esb.jaeger.JaegerResponseLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepeatedPrimitiveProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfRepeatedPrimitiveFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        assertTrue(repeatedPrimitiveProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfRepeatedMessageFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedMessageFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("routes");
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
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(otherFieldDescriptor);

        assertFalse(repeatedPrimitiveProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfCannotHandle() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(otherFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(otherFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, "123");
        assertEquals("", returnedBuilder.getField(otherFieldDescriptor));
    }

    @Test
    public void shouldReturnSameBuilderWithoutSettingFieldIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, null);
        List<Object> outputValues = (List<Object>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldSetEmptyListInBuilderIfEmptyListIfPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, inputValues);
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldSetFieldPassedInTheBuilderAsAList() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, inputValues);
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(inputValues.get(0), outputValues.get(0));
        assertEquals(inputValues.get(1), outputValues.get(1));
    }

    @Test
    public void shouldSetFieldPassedInTheBuilderAsArray() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        DynamicMessage.Builder returnedBuilder = repeatedPrimitiveProtoHandler.transformForKafka(builder, inputValues.toArray());
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(inputValues.get(0), outputValues.get(0));
        assertEquals(inputValues.get(1), outputValues.get(1));
    }

    @Test
    public void shouldReturnArrayOfObjectsWithTypeSameAsFieldDescriptorForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();
        inputValues.add(1);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues);

        assertEquals(String.class, outputValues.get(0).getClass());
    }

    @Test
    public void shouldReturnEmptyArrayOfObjectsIfEmptyListPassedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();

        List<Object> outputValues = (List<Object>) repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues);

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnEmptyArrayOfObjectsIfNullPassedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveProtoHandler.transformFromPostProcessor(null);

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnAllFieldsInAListOfObjectsIfMultipleFieldsPassedWithSameTypeAsFieldDescriptorForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();
        inputValues.add(1);
        inputValues.add(2);
        inputValues.add(3);

        List<Object> outputValues = (List<Object>) repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues);

        assertEquals(3, outputValues.size());
        assertEquals("1", outputValues.get(0));
        assertEquals("2", outputValues.get(1));
        assertEquals("3", outputValues.get(2));
    }

    @Test
    public void shouldThrowExceptionIfFieldDesciptorTypeNotSupportedForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("routes");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        try {
            ArrayList<String> inputValues = new ArrayList<>();
            inputValues.add("test");
            repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues);
        } catch (Exception e) {
            assertEquals(DataTypeNotSupportedException.class, e.getClass());
            assertEquals("Data type MESSAGE not supported in primitive type handlers", e.getMessage());
        }
    }

    @Test
    public void shouldThrowInvalidDataTypeExceptionInCaseOfTypeMismatchForPostProcessorTransform() {
        Descriptors.FieldDescriptor repeatedFloatFieldDescriptor = JaegerResponseLogMessage.getDescriptor().findFieldByName("scores");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFloatFieldDescriptor);
        try {
            ArrayList<String> inputValues = new ArrayList<>();
            inputValues.add("test");
            repeatedPrimitiveProtoHandler.transformFromPostProcessor(inputValues);
        } catch (Exception e) {
            assertEquals(InvalidDataTypeException.class, e.getClass());
            assertEquals("type mismatch of field: scores, expecting DOUBLE type, actual type class java.lang.String", e.getMessage());
        }
    }

    @Test
    public void shouldReturnAllFieldsInAListOfObjectsIfMultipleFieldsPassedWithSameTypeAsFieldDescriptorForKafkaTransform() throws InvalidProtocolBufferException {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);

        GoLifeBookingLogMessage goLifeBookingLogMessage = GoLifeBookingLogMessage
                .newBuilder()
                .addFavouriteServiceProviderGuids("1")
                .addFavouriteServiceProviderGuids("2")
                .addFavouriteServiceProviderGuids("3")
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(GoLifeBookingLogMessage.getDescriptor(), goLifeBookingLogMessage.toByteArray());

        String[] outputValues = (String[]) repeatedPrimitiveProtoHandler.transformFromKafka(dynamicMessage.getField(repeatedFieldDescriptor));

        assertEquals(3, outputValues.length);
        assertEquals("1", outputValues[0]);
        assertEquals("2", outputValues[1]);
        assertEquals("3", outputValues[2]);
    }

    @Test
    public void shouldThrowUnsupportedDataTypeExceptionInCaseOfInCaseOfEnumForKafkaTransform() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("status");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(fieldDescriptor);
        try {
            repeatedPrimitiveProtoHandler.transformFromKafka("CREATED");
        } catch (Exception e) {
            assertEquals(DataTypeNotSupportedException.class, e.getClass());
            assertEquals("Data type ENUM not supported in primitive type handlers", e.getMessage());
        }
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedPrimitiveProtoHandler repeatedPrimitiveProtoHandler = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedPrimitiveProtoHandler.getTypeInformation();
        TypeInformation<String[]> expectedTypeInformation = ObjectArrayTypeInfo.getInfoFor(Types.STRING);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldConvertRepeatedRowDataToJsonString() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        Object value = new RepeatedPrimitiveProtoHandler(repeatedFieldDescriptor).transformToJson(inputValues);
        Assert.assertEquals("[\"test1\",\"test2\"]", String.valueOf(value));
    }
}
