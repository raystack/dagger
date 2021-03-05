package com.gojek.daggers.protohandler.typehandler;

import com.gojek.daggers.exception.DataTypeNotSupportedException;
import com.gojek.esb.ESBLog;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.types.GoFoodShoppingItemProto;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrimitiveTypeHandlerFactoryTest {

    @Test
    public void shouldReturnIntegerTypeHanlderForInteger() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("quantity"));
        assertEquals(IntegerPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnBooleanTypeHanlderForBoolean() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(BookingLogMessage.getDescriptor().findFieldByName("is_reblast"));
        assertEquals(BooleanPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnDoubleTypeHanlderForDouble() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("price"));
        assertEquals(DoublePrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnFloatTypeHanlderForFloat() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(BookingLogMessage.getDescriptor().findFieldByName("customer_price"));
        assertEquals(FloatPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnLongTypeHanlderForLong() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("id"));
        assertEquals(LongPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnStringTypeHanlderForString() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(GoFoodShoppingItemProto.GoFoodShoppingItem.getDescriptor().findFieldByName("name"));
        assertEquals(StringPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnByteStringTypeHanlderForByteString() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(ESBLog.ESBLogMessageEnvelope.getDescriptor().findFieldByName("log_key"));
        assertEquals(ByteStringPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldThrowExceptionIfTypeNotSupported() {
        try {
            PrimitiveTypeHandlerFactory.getTypeHandler(BookingLogMessage.getDescriptor().findFieldByName("status"));
        } catch (Exception e) {
            assertEquals(DataTypeNotSupportedException.class, e.getClass());
            assertEquals("Data type ENUM not supported in primitive type handlers", e.getMessage());
        }
    }
}
