package io.odpf.dagger.common.serde.proto.protohandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.dagger.consumer.TestBookingLogMessage;
import io.odpf.dagger.consumer.TestEnumMessage;
import io.odpf.dagger.consumer.TestRepeatedEnumMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepeatedEnumProtoHandlerTest {
    @Test
    public void shouldReturnTrueIfRepeatedEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(repeatedEnumFieldDescriptor);

        assertTrue(repeatedEnumProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(enumFieldDescriptor);

        assertFalse(repeatedEnumProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanRepeatedEnumTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(otherFieldDescriptor);

        assertFalse(repeatedEnumProtoHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(repeatedEnumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedEnumFieldDescriptor.getContainingType());

        assertEquals(Collections.EMPTY_LIST, repeatedEnumProtoHandler.transformForKafka(builder, 123).getField(repeatedEnumFieldDescriptor));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(repeatedEnumFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedEnumProtoHandler.getTypeInformation();
        TypeInformation<String[]> expectedTypeInformation = ObjectArrayTypeInfo.getInfoFor(Types.STRING);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldTransformValueForPostProcessorAsStringArray() {
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumProtoHandler.transformFromPostProcessor(inputValues);

        assertEquals(inputValues.get(0), outputValues[0]);
        assertEquals(inputValues.get(1), outputValues[1]);
    }

    @Test
    public void shouldTransformValueForPostProcessorAsEmptyStringArrayForNull() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumProtoHandler.transformFromPostProcessor(null);

        assertEquals(0, outputValues.length);
    }

    @Test
    public void shouldTransformValueForKafkaAsStringArray() throws InvalidProtocolBufferException {
        TestRepeatedEnumMessage testRepeatedEnumMessage = TestRepeatedEnumMessage.newBuilder().addTestEnums(TestEnumMessage.Enum.UNKNOWN).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestRepeatedEnumMessage.getDescriptor(), testRepeatedEnumMessage.toByteArray());

        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumProtoHandler.transformFromKafka(dynamicMessage.getField(repeatedEnumFieldDescriptor));

        assertEquals("UNKNOWN", outputValues[0]);
    }

    @Test
    public void shouldTransformValueForKafkaAsEmptyStringArrayForNull() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumProtoHandler repeatedEnumProtoHandler = new RepeatedEnumProtoHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumProtoHandler.transformFromKafka(null);

        assertEquals(0, outputValues.length);
    }
}
