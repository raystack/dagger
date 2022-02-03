package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestNestedRepeatedMessage;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RepeatedStructMessageProtoHandlerTest {

    @Test
    public void shouldReturnTrueForCanHandleForRepeatedRepeatedStructFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(repeatedStructFieldDescriptor);
        assertTrue(repeatedStructMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForStructFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(fieldDescriptor);
        assertFalse(repeatedStructMessageProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForMessageFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("driver_pickup_location");
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
