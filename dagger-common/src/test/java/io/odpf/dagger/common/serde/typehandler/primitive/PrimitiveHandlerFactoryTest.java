package io.odpf.dagger.common.serde.typehandler.primitive;

import io.odpf.dagger.common.exceptions.serde.DataTypeNotSupportedException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestMessageEnvelope;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrimitiveHandlerFactoryTest {

    @Test
    public void shouldReturnIntegerTypeHandlerForInteger() {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id"));
        assertEquals(IntegerTypeHandler.class, primitiveHandler.getClass());
    }

    @Test
    public void shouldReturnBooleanTypeHandlerForBoolean() {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled"));
        assertEquals(BooleanTypeHandler.class, primitiveHandler.getClass());
    }

    @Test
    public void shouldReturnDoubleTypeHandlerForDouble() {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount"));
        assertEquals(DoubleTypeHandler.class, primitiveHandler.getClass());
    }

    @Test
    public void shouldReturnFloatTypeHandlerForFloat() {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash"));
        assertEquals(FloatTypeHandler.class, primitiveHandler.getClass());
    }

    @Test
    public void shouldReturnLongTypeHandlerForLong() {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("customer_total_fare_without_surge"));
        assertEquals(LongTypeHandler.class, primitiveHandler.getClass());
    }

    @Test
    public void shouldReturnStringTypeHandlerForString() {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("order_number"));
        assertEquals(StringTypeHandler.class, primitiveHandler.getClass());
    }

    @Test
    public void shouldReturnByteStringTypeHandlerForByteString() {
        PrimitiveHandler primitiveHandler = PrimitiveHandlerFactory
                .getTypeHandler(TestMessageEnvelope.getDescriptor().findFieldByName("log_key"));
        assertEquals(ByteStringTypeHandler.class, primitiveHandler.getClass());
    }

    @Test
    public void shouldThrowExceptionIfTypeNotSupported() {
        DataTypeNotSupportedException exception = Assert.assertThrows(DataTypeNotSupportedException.class,
                () -> PrimitiveHandlerFactory.getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("status")));
        assertEquals("Data type ENUM not supported in primitive type handlers", exception.getMessage());
    }
}
