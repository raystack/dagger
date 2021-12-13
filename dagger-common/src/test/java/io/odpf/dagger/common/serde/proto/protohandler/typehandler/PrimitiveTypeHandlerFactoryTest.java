package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import io.odpf.dagger.common.exceptions.serde.DataTypeNotSupportedException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestMessageEnvelope;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrimitiveTypeHandlerFactoryTest {

    @Test
    public void shouldReturnIntegerTypeHandlerForInteger() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("cancel_reason_id"));
        assertEquals(IntegerPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnBooleanTypeHandlerForBoolean() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("customer_dynamic_surge_enabled"));
        assertEquals(BooleanPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnDoubleTypeHandlerForDouble() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("cash_amount"));
        assertEquals(DoublePrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnFloatTypeHandlerForFloat() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("amount_paid_by_cash"));
        assertEquals(FloatPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnLongTypeHandlerForLong() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("customer_total_fare_without_surge"));
        assertEquals(LongPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnStringTypeHandlerForString() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("order_number"));
        assertEquals(StringPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldReturnByteStringTypeHandlerForByteString() {
        PrimitiveTypeHandler primitiveTypeHandler = PrimitiveTypeHandlerFactory
                .getTypeHandler(TestMessageEnvelope.getDescriptor().findFieldByName("log_key"));
        assertEquals(ByteStringPrimitiveTypeHandler.class, primitiveTypeHandler.getClass());
    }

    @Test
    public void shouldThrowExceptionIfTypeNotSupported() {
        DataTypeNotSupportedException exception = Assert.assertThrows(DataTypeNotSupportedException.class,
                () -> PrimitiveTypeHandlerFactory.getTypeHandler(TestBookingLogMessage.getDescriptor().findFieldByName("status")));
        assertEquals("Data type ENUM not supported in primitive type handlers", exception.getMessage());
    }
}
