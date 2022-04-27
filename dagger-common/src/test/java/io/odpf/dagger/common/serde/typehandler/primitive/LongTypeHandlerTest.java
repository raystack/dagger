package io.odpf.dagger.common.serde.typehandler.primitive;

import io.odpf.dagger.consumer.TestNestedRepeatedMessage;
import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestAggregatedSupplyMessage;
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

public class LongTypeHandlerTest {

    @Test
    public void shouldHandleLongTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        assertTrue(longTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanLong() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("vehicle_type");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        assertFalse(longTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        Object value = longTypeHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        Object value = longTypeHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeLong() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        Object value = longTypeHandler.parseObject(null);

        assertEquals(0L, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        assertEquals(Types.LONG, longTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        assertEquals(Types.OBJECT_ARRAY(Types.LONG), longTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        ArrayList<Long> inputValues = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
        Object actualValues = longTypeHandler.parseRepeatedObjectField(inputValues);
        assertArrayEquals(inputValues.toArray(), (Long[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        Object actualValues = longTypeHandler.parseRepeatedObjectField(null);
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
        LongTypeHandler longHandler = new LongTypeHandler(fieldDescriptor);

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
        LongTypeHandler longHandler = new LongTypeHandler(fieldDescriptor);

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
        LongTypeHandler longHandler = new LongTypeHandler(fieldDescriptor);

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

        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        long[] actualValue = (long[]) longTypeHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new long[]{6222L, 0L}, actualValue);
    }

    @Test
    public void shouldReturnEmptyLongArrayWhenParseRepeatedSimpleGroupFieldIsCalledWithNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_long_field");

        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        long[] actualValue = (long[]) longTypeHandler.parseRepeatedSimpleGroupField(null);

        assertArrayEquals(new long[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyLongArrayWhenRepeatedInt64FieldInsideSimpleGroupIsNotPresent() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_long_field");

        GroupType parquetSchema = buildMessage()
                .repeated(BOOLEAN).named("some_other_field")
                .named("TestNestedRepeatedMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        long[] actualValue = (long[]) longTypeHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new long[0], actualValue);
    }

    @Test
    public void shouldReturnEmptyLongArrayWhenRepeatedInt64FieldInsideSimpleGroupIsNotInitialized() {
        Descriptors.FieldDescriptor fieldDescriptor = TestNestedRepeatedMessage.getDescriptor().findFieldByName("repeated_long_field");

        GroupType parquetSchema = buildMessage()
                .repeated(INT64).named("repeated_long_field")
                .named("TestNestedRepeatedMessage");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        LongTypeHandler longTypeHandler = new LongTypeHandler(fieldDescriptor);
        long[] actualValue = (long[]) longTypeHandler.parseRepeatedSimpleGroupField(simpleGroup);

        assertArrayEquals(new long[0], actualValue);
    }
}
