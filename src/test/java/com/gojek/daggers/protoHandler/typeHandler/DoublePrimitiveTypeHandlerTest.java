package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.esb.types.GoFoodShoppingItemProto;
import com.google.protobuf.Descriptors;
import org.junit.Test;

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

}