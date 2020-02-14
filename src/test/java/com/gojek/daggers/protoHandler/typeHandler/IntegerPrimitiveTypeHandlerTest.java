package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.esb.types.GoFoodShoppingItemProto;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.*;

public class IntegerPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleIntegerTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        IntegerPrimitiveTypeHandler integerPrimitiveTypeHandler = new IntegerPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(integerPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanInteger() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        IntegerPrimitiveTypeHandler integerPrimitiveTypeHandler = new IntegerPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(integerPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeInteger() {
        int actualValue = 2;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        IntegerPrimitiveTypeHandler integerPrimitiveTypeHandler = new IntegerPrimitiveTypeHandler(fieldDescriptor);
        Object value = integerPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeInteger() {
        int actualValue = 2;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        IntegerPrimitiveTypeHandler integerPrimitiveTypeHandler = new IntegerPrimitiveTypeHandler(fieldDescriptor);
        Object value = integerPrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeInteger() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        IntegerPrimitiveTypeHandler integerPrimitiveTypeHandler = new IntegerPrimitiveTypeHandler(fieldDescriptor);
        Object value = integerPrimitiveTypeHandler.getValue(null);

        assertEquals(0, value);
    }

}