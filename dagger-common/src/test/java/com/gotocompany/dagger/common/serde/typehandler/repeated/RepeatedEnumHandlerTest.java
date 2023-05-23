package com.gotocompany.dagger.common.serde.typehandler.repeated;

import com.gotocompany.dagger.common.core.FieldDescriptorCache;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestEnumMessage;
import com.gotocompany.dagger.consumer.TestRepeatedEnumMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepeatedEnumHandlerTest {
    @Test
    public void shouldReturnTrueIfRepeatedEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);

        assertTrue(repeatedEnumHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfEnumFieldDescriptorIsPassed() {
        Descriptors.FieldDescriptor enumFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("service_type");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(enumFieldDescriptor);

        assertFalse(repeatedEnumHandler.canHandle());
    }

    @Test
    public void shouldReturnFalseIfFieldDescriptorOtherThanRepeatedEnumTypeIsPassed() {
        Descriptors.FieldDescriptor otherFieldDescriptor = TestBookingLogMessage.getDescriptor().findFieldByName("order_number");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(otherFieldDescriptor);

        assertFalse(repeatedEnumHandler.canHandle());
    }

    @Test
    public void shouldReturnTheSameBuilderWithoutSettingAnyValue() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(repeatedEnumFieldDescriptor.getContainingType());

        assertEquals(Collections.EMPTY_LIST, repeatedEnumHandler.transformToProtoBuilder(builder, 123).getField(repeatedEnumFieldDescriptor));
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);
        TypeInformation actualTypeInformation = repeatedEnumHandler.getTypeInformation();
        TypeInformation<String[]> expectedTypeInformation = ObjectArrayTypeInfo.getInfoFor(Types.STRING);
        assertEquals(expectedTypeInformation, actualTypeInformation);
    }

    @Test
    public void shouldTransformValueForPostProcessorAsStringArray() {
        ArrayList<String> inputValues = new ArrayList<>();
        inputValues.add("test1");
        inputValues.add("test2");

        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumHandler.transformFromPostProcessor(inputValues);

        assertEquals(inputValues.get(0), outputValues[0]);
        assertEquals(inputValues.get(1), outputValues[1]);
    }

    @Test
    public void shouldTransformValueForPostProcessorAsEmptyStringArrayForNull() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumHandler.transformFromPostProcessor(null);

        assertEquals(0, outputValues.length);
    }

    @Test
    public void shouldTransformValueFromProtoAsStringArray() throws InvalidProtocolBufferException {
        TestRepeatedEnumMessage testRepeatedEnumMessage = TestRepeatedEnumMessage.newBuilder().addTestEnums(TestEnumMessage.Enum.UNKNOWN).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestRepeatedEnumMessage.getDescriptor(), testRepeatedEnumMessage.toByteArray());

        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumHandler.transformFromProto(dynamicMessage.getField(repeatedEnumFieldDescriptor));

        assertEquals("UNKNOWN", outputValues[0]);
    }

    @Test
    public void shouldTransformValueFromProtoAsEmptyStringArrayForNull() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);

        String[] outputValues = (String[]) repeatedEnumHandler.transformFromProto(null);

        assertEquals(0, outputValues.length);
    }

    @Test
    public void shouldTransformValueFromProtoUsingCacheAsStringArray() throws InvalidProtocolBufferException {
        TestRepeatedEnumMessage testRepeatedEnumMessage = TestRepeatedEnumMessage.newBuilder().addTestEnums(TestEnumMessage.Enum.UNKNOWN).build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(TestRepeatedEnumMessage.getDescriptor(), testRepeatedEnumMessage.toByteArray());

        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestRepeatedEnumMessage.getDescriptor());

        String[] outputValues = (String[]) repeatedEnumHandler.transformFromProtoUsingCache(dynamicMessage.getField(repeatedEnumFieldDescriptor), fieldDescriptorCache);

        assertEquals("UNKNOWN", outputValues[0]);
    }

    @Test
    public void shouldTransformValueFromProtoUsingCacheAsEmptyStringArrayForNull() {
        Descriptors.FieldDescriptor repeatedEnumFieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(repeatedEnumFieldDescriptor);
        FieldDescriptorCache fieldDescriptorCache = new FieldDescriptorCache(TestRepeatedEnumMessage.getDescriptor());

        String[] outputValues = (String[]) repeatedEnumHandler.transformFromProtoUsingCache(null, fieldDescriptorCache);

        assertEquals(0, outputValues.length);
    }

    @Test
    public void shouldTransformValueForParquetAsStringArray() {
        Descriptors.FieldDescriptor fieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(fieldDescriptor);
        String enum1 = String.valueOf(fieldDescriptor.getEnumType().findValueByName("FIRST_ENUM_VALUE"));
        String enum2 = String.valueOf(fieldDescriptor.getEnumType().findValueByName("SECOND_ENUM_VALUE"));

        GroupType parquetSchema = buildMessage()
                .repeated(BINARY).named("test_enums")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("test_enums", enum1);
        simpleGroup.add("test_enums", enum2);

        String[] actualEnumArray = (String[]) repeatedEnumHandler.transformFromParquet(simpleGroup);
        assertEquals("FIRST_ENUM_VALUE", actualEnumArray[0]);
        assertEquals("SECOND_ENUM_VALUE", actualEnumArray[1]);
    }

    @Test
    public void shouldTransformValueForParquetAsEmptyStringArrayWhenNullIsPassedAsArgument() {
        Descriptors.FieldDescriptor fieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(fieldDescriptor);

        String[] expectedEnumArray = (String[]) repeatedEnumHandler.transformFromParquet(null);

        assertArrayEquals(new String[0], expectedEnumArray);
    }

    @Test
    public void shouldSubstituteDefaultEnumStringWhenAnyValueInsideSimpleGroupRepeatedEnumIsNotPresentInProtoDefinition() {
        Descriptors.FieldDescriptor fieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(fieldDescriptor);
        String enum1 = String.valueOf(fieldDescriptor.getEnumType().findValueByName("FIRST_ENUM_VALUE"));
        String enum2 = "some-junk-value";

        GroupType parquetSchema = buildMessage()
                .repeated(BINARY).named("test_enums")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("test_enums", enum1);
        simpleGroup.add("test_enums", enum2);

        String[] actualEnumArray = (String[]) repeatedEnumHandler.transformFromParquet(simpleGroup);
        assertEquals("FIRST_ENUM_VALUE", actualEnumArray[0]);
        assertEquals("UNKNOWN", actualEnumArray[1]);
    }

    @Test
    public void shouldTransformValueForParquetAsEmptyStringArrayWhenFieldIsNotPresentInsideSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(fieldDescriptor);

        GroupType parquetSchema = buildMessage()
                .repeated(INT64).named("first_name")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("first_name", 34L);

        String[] actualEnumArray = (String[]) repeatedEnumHandler.transformFromParquet(simpleGroup);

        assertArrayEquals(new String[0], actualEnumArray);
    }

    @Test
    public void shouldTransformValueForParquetAsEmptyStringArrayWhenFieldIsNotInitializedInsideSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestRepeatedEnumMessage.getDescriptor().findFieldByName("test_enums");
        RepeatedEnumHandler repeatedEnumHandler = new RepeatedEnumHandler(fieldDescriptor);

        GroupType parquetSchema = buildMessage()
                .repeated(BINARY).named("test_enums")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        String[] actualEnumArray = (String[]) repeatedEnumHandler.transformFromParquet(simpleGroup);

        assertArrayEquals(new String[0], actualEnumArray);
    }
}
