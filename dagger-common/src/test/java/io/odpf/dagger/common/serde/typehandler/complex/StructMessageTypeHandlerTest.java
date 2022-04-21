package io.odpf.dagger.common.serde.typehandler.complex;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestRepeatedEnumMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StructMessageTypeHandlerTest {

    @Test
    public void shouldReturnTrueForCanHandleForStructFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageTypeHandler structMessageTypeHandler = new StructMessageTypeHandler(fieldDescriptor);
        assertTrue(structMessageTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForRepeatedStructFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        StructMessageTypeHandler structMessageTypeHandler = new StructMessageTypeHandler(repeatedEnumFieldDescriptor);
        assertFalse(structMessageTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForMessageFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("driver_pickup_location");
        StructMessageTypeHandler structMessageTypeHandler = new StructMessageTypeHandler(fieldDescriptor);
        assertFalse(structMessageTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageTypeHandler structMessageTypeHandler = new StructMessageTypeHandler(fieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(fieldDescriptor.getContainingType());
        assertEquals(DynamicMessage.getDefaultInstance(fieldDescriptor.getContainingType()).getAllFields().size(),
                ((DynamicMessage) structMessageTypeHandler.transformToProtoBuilder(builder, 123).getField(fieldDescriptor)).getAllFields().size());
    }

    @Test
    public void shouldReturnNullForTransformForPostProcessor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageTypeHandler structMessageTypeHandler = new StructMessageTypeHandler(fieldDescriptor);
        assertNull(structMessageTypeHandler.transformFromPostProcessor("test"));
    }

    @Test
    public void shouldReturnNullForTransformForKafka() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageTypeHandler structMessageTypeHandler = new StructMessageTypeHandler(fieldDescriptor);
        assertNull(structMessageTypeHandler.transformFromProto("test"));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageTypeHandler structMessageTypeHandler = new StructMessageTypeHandler(fieldDescriptor);
        TypeInformation actualTypeInformation = structMessageTypeHandler.getTypeInformation();
        TypeInformation<Row> expectedTypeInformation = Types.ROW_NAMED(new String[]{});
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldReturnNullWhenTransformFromParquetIsCalledWithAnyArgument() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        StructMessageTypeHandler protoHandler = new StructMessageTypeHandler(fieldDescriptor);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertNull(protoHandler.transformFromParquet(simpleGroup));
    }
}
