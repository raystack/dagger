package com.gotocompany.dagger.common.serde.typehandler.complex;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestRepeatedEnumMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StructMessageHandlerTest {

    @Test
    public void shouldReturnTrueForCanHandleForStructFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageHandler structMessageHandler = new StructMessageHandler(fieldDescriptor);
        assertTrue(structMessageHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForRepeatedStructFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        StructMessageHandler structMessageHandler = new StructMessageHandler(repeatedEnumFieldDescriptor);
        assertFalse(structMessageHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForMessageFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("driver_pickup_location");
        StructMessageHandler structMessageHandler = new StructMessageHandler(fieldDescriptor);
        assertFalse(structMessageHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageHandler structMessageHandler = new StructMessageHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());
        assertEquals(DynamicMessage.getDefaultInstance(fieldDescriptor.getContainingType()).getAllFields().size(),
                ((DynamicMessage) structMessageHandler.transformToProtoBuilder(builder, 123).getField(fieldDescriptor)).getAllFields().size());
    }

    @Test
    public void shouldReturnNullForTransformForPostProcessor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageHandler structMessageHandler = new StructMessageHandler(fieldDescriptor);
        assertNull(structMessageHandler.transformFromPostProcessor("test"));
    }

    @Test
    public void shouldReturnNullForTransformForKafka() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageHandler structMessageHandler = new StructMessageHandler(fieldDescriptor);
        assertNull(structMessageHandler.transformFromProto("test"));
    }

    @Test
    public void shouldReturnNullForTransformFromProtoUsingCache() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageHandler structMessageHandler = new StructMessageHandler(fieldDescriptor);
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestBookingLogMessage.getDescriptor());

        assertNull(structMessageHandler.transformFromProtoUsingCache("test", fieldDescriptorCache));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageHandler structMessageHandler = new StructMessageHandler(fieldDescriptor);
        TypeInformation actualTypeInformation = structMessageHandler.getTypeInformation();
        TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(new String[]{});
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldReturnNullWhenTransformFromParquetIsCalledWithAnyArgument() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageHandler protoHandler = new StructMessageHandler(fieldDescriptor);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertNull(protoHandler.transformFromParquet(simpleGroup));
    }
}
