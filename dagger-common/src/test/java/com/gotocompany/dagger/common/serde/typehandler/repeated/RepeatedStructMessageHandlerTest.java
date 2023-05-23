package com.gotocompany.dagger.common.serde.typehandler.repeated;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestNestedRepeatedMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RepeatedStructMessageHandlerTest {

    @Test
    public void shouldReturnTrueForCanHandleForRepeatedRepeatedStructFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageHandler repeatedStructMessageHandler = new RepeatedStructMessageHandler(repeatedStructFieldDescriptor);
        assertTrue(repeatedStructMessageHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForStructFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        RepeatedStructMessageHandler repeatedStructMessageHandler = new RepeatedStructMessageHandler(fieldDescriptor);
        assertFalse(repeatedStructMessageHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForMessageFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("driver_pickup_location");
        RepeatedStructMessageHandler repeatedStructMessageHandler = new RepeatedStructMessageHandler(fieldDescriptor);
        assertFalse(repeatedStructMessageHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageHandler repeatedStructMessageHandler = new RepeatedStructMessageHandler(repeatedStructFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedStructFieldDescriptor.getContainingType());
        assertEquals(Collections.EMPTY_LIST, repeatedStructMessageHandler.transformToProtoBuilder(builder, 123).getField(repeatedStructFieldDescriptor));
    }

    @Test
    public void shouldReturnNullForTransformForPostProcessor() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageHandler repeatedStructMessageHandler = new RepeatedStructMessageHandler(repeatedStructFieldDescriptor);
        assertNull(repeatedStructMessageHandler.transformFromPostProcessor("test"));
    }

    @Test
    public void shouldReturnNullForTransformForKafka() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageHandler repeatedStructMessageHandler = new RepeatedStructMessageHandler(repeatedStructFieldDescriptor);
        assertNull(repeatedStructMessageHandler.transformFromProto("test"));
    }

    @Test
    public void shouldReturnNullForTransformFromProtoUsingCache() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestNestedRepeatedMessage.getDescriptor());

        RepeatedStructMessageHandler repeatedStructMessageHandler = new RepeatedStructMessageHandler(repeatedStructFieldDescriptor);
        assertNull(repeatedStructMessageHandler.transformFromProtoUsingCache("test", fieldDescriptorCache));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageHandler repeatedStructMessageHandler = new RepeatedStructMessageHandler(repeatedStructFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedStructMessageHandler.getTypeInformation();
        TypeInformation<Row[]> expectedTypeInformation = Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{}));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldReturnNullWhenTransformFromParquetIsCalledWithAnyArgument() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageHandler protoHandler = new RepeatedStructMessageHandler(fieldDescriptor);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertNull(protoHandler.transformFromParquet(simpleGroup));
    }
}
