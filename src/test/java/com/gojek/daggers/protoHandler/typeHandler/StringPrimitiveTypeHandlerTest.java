package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.esb.types.GoFoodShoppingItemProto;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.*;

public class StringPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleStringTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(stringPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanString() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(stringPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeString() {
        String actualValue = "test";

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        Object value = stringPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeString() {
        Integer actualValue = 23;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        Object value = stringPrimitiveTypeHandler.getValue(actualValue);

        assertEquals("23", value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeString() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        Object value = stringPrimitiveTypeHandler.getValue(null);

        assertEquals("", value);
    }

}