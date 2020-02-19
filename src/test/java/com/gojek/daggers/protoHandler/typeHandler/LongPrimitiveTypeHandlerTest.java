package com.gojek.daggers.protoHandler.typeHandler;

import com.gojek.esb.types.GoFoodShoppingItemProto;
import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.*;

public class LongPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleLongTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(longPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanLong() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(longPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.getValue(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.getValue(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeLong() {
        Descriptors.FieldDescriptor fieldDescriptor = GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.getValue(null);

        assertEquals(0L, value);
    }

}