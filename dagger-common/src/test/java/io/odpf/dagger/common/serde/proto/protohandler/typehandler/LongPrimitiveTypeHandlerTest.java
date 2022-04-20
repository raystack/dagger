package io.odpf.dagger.common.serde.proto.protohandler.typehandler;

import org.apache.flink.api.common.typeinfo.Types;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.consumer.TestAggregatedSupplyMessage;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.GroupType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongPrimitiveTypeHandlerTest {

    @Test
    public void shouldHandleLongTypes() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertTrue(longPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldNotHandleTypesOtherThanLong() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("vehicle_type");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertFalse(longPrimitiveTypeHandler.canHandle());
    }

    @Test
    public void shouldFetchValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.parseObject(actualValue);

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchParsedValueForFieldForFieldDescriptorOfTypeLong() {
        long actualValue = 2L;

        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.parseObject(String.valueOf(actualValue));

        assertEquals(actualValue, value);
    }

    @Test
    public void shouldFetchDefaultValueIfValueNotPresentForFieldDescriptorOfTypeLong() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object value = longPrimitiveTypeHandler.parseObject(null);

        assertEquals(0L, value);
    }

    @Test
    public void shouldReturnTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.LONG, longPrimitiveTypeHandler.getTypeInformation());
    }

    @Test
    public void shouldReturnArrayTypeInformation() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        assertEquals(Types.OBJECT_ARRAY(Types.LONG), longPrimitiveTypeHandler.getArrayType());
    }

    @Test
    public void shouldReturnArrayValues() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        ArrayList<Long> inputValues = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
        Object actualValues = longPrimitiveTypeHandler.getArray(inputValues);
        assertArrayEquals(inputValues.toArray(), (Long[]) actualValues);
    }

    @Test
    public void shouldReturnEmptyArrayOnNull() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        LongPrimitiveTypeHandler longPrimitiveTypeHandler = new LongPrimitiveTypeHandler(fieldDescriptor);
        Object actualValues = longPrimitiveTypeHandler.getArray(null);
        assertEquals(0, ((Long[]) actualValues).length);
    }

    @Test
    public void shouldFetchParsedValueForFieldOfTypeLongInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT64).named("s2_id")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("s2_id", Long.MIN_VALUE);
        LongPrimitiveTypeHandler longHandler = new LongPrimitiveTypeHandler(fieldDescriptor);

        Object actualValue = longHandler.parseSimpleGroup(simpleGroup);

        assertEquals(Long.MIN_VALUE, actualValue);
    }

    @Test
    public void shouldFetchDefaultValueIfFieldNotPresentInSimpleGroup() {
        Descriptors.FieldDescriptor fieldDescriptor = TestAggregatedSupplyMessage.getDescriptor().findFieldByName("s2_id");

        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(INT64).named("some-other-field")
                .named("TestGroupType");
        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        LongPrimitiveTypeHandler longHandler = new LongPrimitiveTypeHandler(fieldDescriptor);

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
        LongPrimitiveTypeHandler longHandler = new LongPrimitiveTypeHandler(fieldDescriptor);

        Object actualValue = longHandler.parseSimpleGroup(simpleGroup);

        assertEquals(0L, actualValue);
    }
}
