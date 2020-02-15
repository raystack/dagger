package com.gojek.daggers.protoHandler;

import com.gojek.daggers.exception.DataTypeNotSupportedException;
import com.gojek.daggers.exception.InvalidDataTypeException;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.booking.GoLifeBookingLogMessage;
import com.gojek.esb.jaeger.JaegerResponseLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class RepeatedProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);

        assertTrue(repeatedProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(otherFieldDescriptor);

        assertFalse(repeatedProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnSameBuilderIfNullFieldIsPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = repeatedProtoHandler.getProtoBuilder(builder, null);
        assertEquals(builder, returnedBuilder);
    }

    @Test
    public void shouldSetEmptyListInBuilderIfEmptyListIfPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();

        DynamicMessage.Builder returnedBuilder = repeatedProtoHandler.getProtoBuilder(builder, inputValues);
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldSetFieldPassedInTheBuilderAsAList() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedFieldDescriptor.getContainingType());

        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        DynamicMessage.Builder returnedBuilder = repeatedProtoHandler.getProtoBuilder(builder, inputValues);
        List<String> outputValues = (List<String>) returnedBuilder.getField(repeatedFieldDescriptor);
        assertEquals(inputValues.get(0), outputValues.get(0));
        assertEquals(inputValues.get(1), outputValues.get(1));
    }

    @Test
    public void shouldReturnArrayOfObjectsWithTypeSameAsFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();
        inputValues.add(1);

        List<Object> outputValues = (List<Object>) repeatedProtoHandler.getTypeAppropriateValue(inputValues);

        assertEquals(String.class, outputValues.get(0).getClass());
    }

    @Test
    public void shouldReturnEmptyArrayOfObjectsIfEmptyListPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();

        List<Object> outputValues = (List<Object>) repeatedProtoHandler.getTypeAppropriateValue(inputValues);

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnEmptyArrayOfObjectsIfNullPassed() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);

        List<Object> outputValues = (List<Object>) repeatedProtoHandler.getTypeAppropriateValue(null);

        assertEquals(0, outputValues.size());
    }

    @Test
    public void shouldReturnAllFieldsInAListOfObjectsIfMultipleFieldsPassedWithSameTypeAsFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("favourite_service_provider_guids");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);

        ArrayList<Integer> inputValues = new ArrayList<>();
        inputValues.add(1);
        inputValues.add(2);
        inputValues.add(3);

        List<Object> outputValues = (List<Object>) repeatedProtoHandler.getTypeAppropriateValue(inputValues);

        assertEquals(3, outputValues.size());
        assertEquals("1", outputValues.get(0));
        assertEquals("2", outputValues.get(1));
        assertEquals("3", outputValues.get(2));
    }

    @Test
    public void shouldThrowExceptionIfFieldDesciptorTypeNotSupported() {
        Descriptors.FieldDescriptor repeatedFieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("routes");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFieldDescriptor);
        try {
            ArrayList<String> inputValues = new ArrayList<>();
            inputValues.add("test");
            repeatedProtoHandler.getTypeAppropriateValue(inputValues);
        } catch (Exception e) {
            assertEquals(DataTypeNotSupportedException.class, e.getClass());
            assertEquals("Data type MESSAGE not supported in primitive type handlers", e.getMessage());
        }
    }

    @Test
    public void shouldThrowInvalidDataTypeExceptionInCaseOfTypeMismatch() {
        Descriptors.FieldDescriptor repeatedFloatFieldDescriptor = JaegerResponseLogMessage.getDescriptor().findFieldByName("scores");
        RepeatedProtoHandler repeatedProtoHandler = new RepeatedProtoHandler(repeatedFloatFieldDescriptor);
        try {
            ArrayList<String> inputValues = new ArrayList<>();
            inputValues.add("test");
            repeatedProtoHandler.getTypeAppropriateValue(inputValues);
        } catch (Exception e) {
            assertEquals(InvalidDataTypeException.class, e.getClass());
            assertEquals("type mismatch of inputField: scores, expecting DOUBLE type, actual type class java.lang.String", e.getMessage());
        }
    }
}