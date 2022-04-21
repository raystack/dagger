package io.odpf.dagger.common.serde.typehandler.repeated;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestEnumMessage;
import io.odpf.dagger.consumer.TestRepeatedEnumMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.*;

public class RepeatedEnumTypeHandlerTest {
    @Test
    public void shouldReturnTrueIfRepeatedEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(repeatedEnumFieldDescriptor);

        assertTrue(repeatedEnumTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(enumFieldDescriptor);

        assertFalse(repeatedEnumTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanRepeatedEnumTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(otherFieldDescriptor);

        assertFalse(repeatedEnumTypeHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(repeatedEnumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedEnumFieldDescriptor.getContainingType());

        assertEquals(Collections.EMPTY_LIST, repeatedEnumTypeHandler.transformToProtoBuilder(builder, 123).getField(repeatedEnumFieldDescriptor));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(repeatedEnumFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedEnumTypeHandler.getTypeInformation();
        TypeInformation<String[]> expectedTypeInformation = ObjectArrayTypeInfo.getInfoFor(Types.STRING);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldTransformValueForPostProcessorAsStringArray() {
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumTypeHandler.transformFromPostProcessor(inputValues);

        assertEquals(inputValues.get(0), outputValues[0]);
        assertEquals(inputValues.get(1), outputValues[1]);
    }

    @Test
    public void shouldTransformValueForPostProcessorAsEmptyStringArrayForNull() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumTypeHandler.transformFromPostProcessor(null);

        assertEquals(0, outputValues.length);
    }

    @Test
    public void shouldTransformValueForKafkaAsStringArray() throws InvalidProtocolBufferException {
        TestRepeatedEnumMessage testRepeatedEnumMessage = TestRepeatedEnumMessage.newBuilder().addTestEnums(TestEnumMessage.Enum.UNKNOWN).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestRepeatedEnumMessage.getDescriptor(), testRepeatedEnumMessage.toByteArray());

        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumTypeHandler.transformFromProto(dynamicMessage.getField(repeatedEnumFieldDescriptor));

        assertEquals("UNKNOWN", outputValues[0]);
    }

    @Test
    public void shouldTransformValueForKafkaAsEmptyStringArrayForNull() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumTypeHandler repeatedEnumTypeHandler = new RepeatedEnumTypeHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumTypeHandler.transformFromProto(null);

        assertEquals(0, outputValues.length);
    }

    @Test
    public void shouldReturnNullWhenTransformFromParquetIsCalledWithAnyArgument() {
        Descriptors.FieldDescriptor fieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumTypeHandler protoHandler = new RepeatedEnumTypeHandler(fieldDescriptor);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        assertNull(protoHandler.transformFromParquet(simpleGroup));
    }
}
