package io.odpf.dagger.common.serde.typehandler.repeated;

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

public class RepeatedStructMessageTypeHandlerTest {

    @Test
    public void shouldReturnTrueForCanHandleForRepeatedRepeatedStructFieldDescriptor() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageTypeHandler repeatedStructMessageTypeHandler = new RepeatedStructMessageTypeHandler(repeatedStructFieldDescriptor);
        assertTrue(repeatedStructMessageTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForStructFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("profile_data");
        RepeatedStructMessageTypeHandler repeatedStructMessageTypeHandler = new RepeatedStructMessageTypeHandler(fieldDescriptor);
        assertFalse(repeatedStructMessageTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseForCanHandleForMessageFieldDescriptor() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("driver_pickup_location");
        RepeatedStructMessageTypeHandler repeatedStructMessageTypeHandler = new RepeatedStructMessageTypeHandler(fieldDescriptor);
        assertFalse(repeatedStructMessageTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageTypeHandler repeatedStructMessageTypeHandler = new RepeatedStructMessageTypeHandler(repeatedStructFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedStructFieldDescriptor.getContainingType());
        assertEquals(Collections.EMPTY_LIST, repeatedStructMessageTypeHandler.transformToProtoBuilder(builder, 123).getField(repeatedStructFieldDescriptor));
    }

    @Test
    public void shouldReturnNullForTransformForPostProcessor() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageTypeHandler repeatedStructMessageTypeHandler = new RepeatedStructMessageTypeHandler(repeatedStructFieldDescriptor);
        assertNull(repeatedStructMessageTypeHandler.transformFromPostProcessor("test"));
    }

    @Test
    public void shouldReturnNullForTransformForKafka() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageTypeHandler repeatedStructMessageTypeHandler = new RepeatedStructMessageTypeHandler(repeatedStructFieldDescriptor);
        assertNull(repeatedStructMessageTypeHandler.transformFromProto("test"));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedStructFieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageTypeHandler repeatedStructMessageTypeHandler = new RepeatedStructMessageTypeHandler(repeatedStructFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedStructMessageTypeHandler.getTypeInformation();
        TypeInformation<Row[]> expectedTypeInformation = Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[]{}));
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldReturnNullWhenTransformFromParquetIsCalledWithAnyArgument() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("metadata");
        RepeatedStructMessageTypeHandler protoHandler = new RepeatedStructMessageTypeHandler(fieldDescriptor);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertNull(protoHandler.transformFromParquet(simpleGroup));
    }

}
