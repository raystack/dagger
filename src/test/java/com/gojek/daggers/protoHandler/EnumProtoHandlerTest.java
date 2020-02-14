package com.gojek.daggers.protoHandler;

import com.gojek.esb.booking.BookingLogMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Test;

import static com.gojek.esb.types.ServiceTypeProto.ServiceType.Enum.GO_LIFE;
import static com.gojek.esb.types.ServiceTypeProto.ServiceType.Enum.GO_RIDE;
import static org.junit.Assert.*;

public class EnumProtoHandlerTest {

    @Test
    public void shouldReturnTrueIfEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(enumFieldDescriptor);

        assertTrue(enumProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanEnumTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(otherFieldDescriptor);

        assertFalse(enumProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderIfCanNotHandle() {
        Descriptors.FieldDescriptor fieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("order_number");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());

        assertEquals(builder, enumProtoHandler.getProtoBuilder(builder, 123));
    }

    @Test
    public void shouldSetTheFieldPassedInTheBuilderForEnumFieldTypeDescriptor() {
        Descriptors.FieldDescriptor enumFieldDescriptor = BookingLogMessage.getDescriptor().findFieldByName("service_type");
        EnumProtoHandler enumProtoHandler = new EnumProtoHandler(enumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(enumFieldDescriptor.getContainingType());

        DynamicMessage.Builder returnedBuilder = enumProtoHandler.getProtoBuilder(builder, "GO_LIFE");
        assertEquals(GO_LIFE, returnedBuilder.getField(enumFieldDescriptor));
    }
}