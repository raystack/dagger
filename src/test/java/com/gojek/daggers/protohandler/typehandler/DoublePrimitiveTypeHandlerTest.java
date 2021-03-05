package com.gojek.daggers.protohandler.typehandler;

import com.gojek.esb.types.GoFoodShoppingItemProto;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class DoublePrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleDoubleTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        assertTrue(doublePrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanDouble() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        assertFalse(doublePrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeDouble() {
        double actualValue = 2.0D;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        Object value = doublePrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeDouble() {
        double actualValue = 2.0D;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        Object value = doublePrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeDouble() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        Object value = doublePrimitiveTypeHandler.getValue(null);

        assertEquals(0.0D, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.DOUBLE, doublePrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.PRIMITIVE_ARRAY(Types.DOUBLE), doublePrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        ArrayList<Double> inputValues = new ArrayList<>(Arrays.asList(1D, 2D, 3D));
        Object actualValues = doublePrimitiveTypeHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (Double[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price");
        DoublePrimitiveTypeHandler doublePrimitiveTypeHandler = new DoublePrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = doublePrimitiveTypeHandler.getArray(null);
        assertEquals(0, ((Double[]) actualValues).length);
    }

}
