package com.gojek.daggers.protoHandler;

import com.gojek.esb.booking.GoLifeBookingLogMessage;
import com.gojek.esb.clevertap.ClevertapEventLogMessage;
import com.gojek.esb.consumer.TestRepeatedEnumMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.*;

public class StructMessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueForCanHandleForStructFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = ClevertapEventLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        assertTrue(structMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForRepeatedStructFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(repeatedEnumFieldDescriptor);
        assertFalse(structMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForMessageFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("driver_eta_pickup");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        assertFalse(structMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor fieldDescriptor = ClevertapEventLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());
        assertEquals(DynamicMessage.getDefaultInstance(fieldDescriptor.getContainingType()).getAllFields().size(),
                ((DynamicMessage) structMessageProtoHandler.populateBuilder(builder, 123).getField(fieldDescriptor)).getAllFields().size());
    }

    @Test
    public void shouldReturnNullForTransformForPostProcessor() {
        Descriptors.FieldDescriptor fieldDescriptor = ClevertapEventLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        assertNull(structMessageProtoHandler.transformForPostProcessor("test"));
    }

    @Test
    public void shouldReturnNullForTransformForKafka() {
        Descriptors.FieldDescriptor fieldDescriptor = ClevertapEventLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        assertNull(structMessageProtoHandler.transformForKafka("test"));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = ClevertapEventLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        TypeInformation actualTypeInformation = structMessageProtoHandler.getTypeInformation();
        TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(new String[]{});
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

}