package org.raystack.dagger.common.serde.typehandler.primitive;

import org.raystack.dagger.consumer.TestNestedRepeatedMessage;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import org.raystack.dagger.consumer.TestAggregatedSupplyMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongHandlerTest {

    @Test
    public void shouldHandleLongTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        assertTrue(longHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanLong() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("vehicle_type");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        assertFalse(longHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        Object value = longHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        Object value = longHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeLong() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        Object value = longHandler.parseObject(null);

        assertEquals(0L, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        assertEquals(Types.LONG, longHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        assertEquals(Types.OBJECT_ARRAY(Types.LONG), longHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        ArrayList<Long> inputValues = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
        Object actualValues = longHandler.parseRepeatedObjectField(inputValues);
        assertArrayEquals(inputValues.toArray(), (Long[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongHandler longHandler = new LongHandler(fieldDescriptor);
        Object actualValues = longHandler.parseRepeatedObjectField(null);
        assertEquals(0, ((Long[]) actualValues).length);
    }

    @Test
    public void shouldFetchParsedValueForFieldOfTypeLongInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT64).named("s2_id")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("s2_id", 101828L);
        LongHandler longHandler = new LongHandler(fieldDescriptor);

        Object actualValue = longHandler.parseSimpleGroup(simpleGroup);

        assertEquals(101828L, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotPresentInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");

        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT64).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        LongHandler longHandler = new LongHandler(fieldDescriptor);

        Object actualValue = longHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0L, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotInitializedWithAValueInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");

        /* The field is added to the schema but not assigned a value */
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT64).named("s2_id")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        LongHandler longHandler = new LongHandler(fieldDescriptor);

        Object actualValue = longHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0L, actualValue);
    }

    @Test
    public void shouldReturnArrayOfLongValuesForFieldOfTypeRepeatedInt64InsideSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_long_field");

        GroupType parquetSchema = buildMessage()
                .repeated(INT64).named("repeated_long_field")
                .named("TestNestedRepeatedMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        simpleGroup.add("repeated_long_field", 6222L);
        simpleGroup.add("repeated_long_field", 0L);

        LongHandler longHandler = new LongHandler(fieldDescriptor);
        Long[] actualValue = (Long[]) longHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new Long[]{6222L, 0L}, actualValue);
    }

    @Test
    public void shouldReturnEmptyLongArrayWhenParseRepeatedSimpleGroupFieldIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_long_field");

        LongHandler longHandler = new LongHandler(fieldDescriptor);
        Long[] actualValue = (Long[]) longHandler.parseRepeatedSimpleGroupField(null);

        assertArrayEquals(new Long[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyLongArrayWhenRepeatedInt64FieldInsideSimpleGroupIsNotPresent() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_long_field");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("some_other_field")
                .named("TestNestedRepeatedMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        LongHandler longHandler = new LongHandler(fieldDescriptor);
        Long[] actualValue = (Long[]) longHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new Long[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyLongArrayWhenRepeatedInt64FieldInsideSimpleGroupIsNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_long_field");

        GroupType parquetSchema = buildMessage()
                .repeated(INT64).named("repeated_long_field")
                .named("TestNestedRepeatedMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        LongHandler longHandler = new LongHandler(fieldDescriptor);
        Long[] actualValue = (Long[]) longHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new Long[0], actualValue);
    }
}
