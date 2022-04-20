package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestNestedRepeatedMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
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
        assertEquals(Collections.EMPTY_LIST, repeatedStructMessageProtoHandler.transformToProtoBuilder(builder, 123).getField(repeatedStructFieldDescriptor));
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
        assertNull(repeatedStructMessageProtoHandler.transformFromProto("test"));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageProtoHandler repeatedStructMessageProtoHandler = new RepeatedStructMessageProtoHandler(repeatedStructFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedStructMessageProtoHandler.getTypeInformation();
        TypeInformation<Row[]> expectedTypeInformation = Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{}));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldReturnNullWhenTransformFromParquetIsCalledWithAnyArgument() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageProtoHandler protoHandler = new RepeatedStructMessageProtoHandler(fieldDescriptor);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertNull(protoHandler.transformFromParquet(simpleGroup));
    }

}
