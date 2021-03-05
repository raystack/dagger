package com.gojek.daggers.protohandler;

import com.gojek.esb.booking.GoLifeBookingLogMessage;
import com.gojek.esb.clevertap.ClevertapEventLogMessage;
import com.gojek.esb.consumer.TestNestedRepeatedMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class RepeatedStructMessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueForCanHandleForRepeatedRepeatedStructFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(repeatedStructFieldDescriptor);
        assertTrue(repeatedStructMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForStructFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = ClevertapEventLogMessage.getDescriptor().findFieldByName("profile_data");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(fieldDescriptor);
        assertFalse(repeatedStructMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForMessageFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = GoLifeBookingLogMessage.getDescriptor().findFieldByName("driver_eta_pickup");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(fieldDescriptor);
        assertFalse(repeatedStructMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(repeatedStructFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedStructFieldDescriptor.getContainingType());
        assertEquals(Collections.EMPTY_LIST, repeatedStructMessageProtoHandler.transformForKafka(builder, 123).getField(repeatedStructFieldDescriptor));
    }

    @Test
    public void shouldReturnNullForTransformForPostProcessor() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(repeatedStructFieldDescriptor);
        assertNull(repeatedStructMessageProtoHandler.transformFromPostProcessor("test"));
    }

    @Test
    public void shouldReturnNullForTransformForKafka() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(repeatedStructFieldDescriptor);
        assertNull(repeatedStructMessageProtoHandler.transformFromKafka("test"));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(repeatedStructFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedStructMessageProtoHandler.getTypeInformation();
        TypeInformation<Row[]> expectedTypeInformation = Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{}));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

}
