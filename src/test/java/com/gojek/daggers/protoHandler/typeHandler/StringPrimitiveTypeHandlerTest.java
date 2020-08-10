package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.esb.types.GoFoodShoppingItemProto;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

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

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.STRING, stringPrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(ObjectArrayTypeInfo.getInfoFor(Types.STRING), stringPrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        ArrayList<String> inputValues = new ArrayList<>(Arrays.asList("1", "2", "3"));
        Object actualValues = stringPrimitiveTypeHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (String[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name");
        StringPrimitiveTypeHandler stringPrimitiveTypeHandler = new StringPrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = stringPrimitiveTypeHandler.getArray(null);
        assertEquals(0, ((String[]) actualValues).length);
    }

}