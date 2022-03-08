package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestRepeatedEnumMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StructMessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueForCanHandleForStructFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
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
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("driver_pickup_location");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        assertFalse(structMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());
        assertEquals(DynamicMessage.getDefaultInstance(fieldDescriptor.getContainingType()).getAllFields().size(),
                ((DynamicMessage) structMessageProtoHandler.transformForKafka(builder, 123).getField(fieldDescriptor)).getAllFields().size());
    }

    @Test
    public void shouldReturnNullForTransformForPostProcessor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        assertNull(structMessageProtoHandler.transformFromPostProcessor("test"));
    }

    @Test
    public void shouldReturnNullForTransformForKafka() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        assertNull(structMessageProtoHandler.transformFromKafka("test"));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageProtoHandler structMessageProtoHandler = new StructMessageProtoHandler(fieldDescriptor);
        TypeInformation actualTypeInformation = structMessageProtoHandler.getTypeInformation();
        TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(new String[]{});
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

}
