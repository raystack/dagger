package io.odpf.dagger.common.serde.parquet.deserialization;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.exceptions.serde.DaggerDeserializationException;
import io.odpf.dagger.consumer.TestBookingLogKey;
import io.odpf.dagger.consumer.TestPrimitiveMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.Timestamp;
import java.time.Instant;

import static io.odpf.dagger.common.core.Constants.*;
import static io.odpf.dagger.common.core.Constants.SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT;
import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SimpleGroupDeserializerTest {

    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private Configuration configuration;

    @Before
    public void setUp() {
        initMocks(this);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }

    @Test
    public void shouldReturnProducedType() {
        SimpleGroupDeserializer simpleGroupDeserializer = new SimpleGroupDeserializer(TestBookingLogKey.class.getTypeName(), 5, "rowtime", stencilClientOrchestrator);
        TypeInformation<Row> producedType = simpleGroupDeserializer.getProducedType();
        assertArrayEquals(
                new String[]{"service_type", "order_number", "order_url", "status", "event_timestamp", INTERNAL_VALIDATION_FIELD_KEY, "rowtime"},
                ((RowTypeInfo) producedType).getFieldNames());
        assertArrayEquals(
                new TypeInformation[]{STRING, STRING, STRING, STRING, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT), BOOLEAN, SQL_TIMESTAMP},
                ((RowTypeInfo) producedType).getFieldTypes());
    }

    @Test
    public void shouldDeserializeSimpleGroupOfPrimitiveTypesIntoRow() {
        SimpleGroupDeserializer simpleGroupDeserializer = new SimpleGroupDeserializer(TestPrimitiveMessage.class.getTypeName(), 9, "rowtime", stencilClientOrchestrator);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("is_valid")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_number")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_hash")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("latitude")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("longitude")
                .required(PrimitiveType.PrimitiveTypeName.FLOAT).named("price")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("packet_count")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("phone")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("event_timestamp")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("service_type")
                .named("TestGroupType");

        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        simpleGroup.add("is_valid", true);
        simpleGroup.add("order_number", "ORDER_1322432");
        String byteString = "g362vxv3ydg73g2ss";
        simpleGroup.add("order_hash", Binary.fromConstantByteArray(byteString.getBytes()));
        simpleGroup.add("latitude", Double.MAX_VALUE);
        simpleGroup.add("longitude", Double.MIN_VALUE);
        simpleGroup.add("price", Float.MAX_VALUE);
        simpleGroup.add("packet_count", Integer.MAX_VALUE);
        simpleGroup.add("phone", Long.MAX_VALUE);
        long currentTimeInMillis = Instant.now().toEpochMilli();
        long seconds = Instant.ofEpochMilli(currentTimeInMillis).getEpochSecond();
        int nanos = Instant.ofEpochMilli(currentTimeInMillis).getNano();
        simpleGroup.add("event_timestamp", currentTimeInMillis);
        simpleGroup.add("service_type", "GO_RIDE");

        Row row = simpleGroupDeserializer.deserialize(simpleGroup);

        assertEquals("ORDER_1322432", row.getField(getProtoIndex("order_number")));
        assertEquals(Float.MAX_VALUE, row.getField(getProtoIndex("price")));
        Row actualTimestampRow = (Row) row.getField(getProtoIndex("event_timestamp"));
        assertNotNull(actualTimestampRow);
        assertEquals(seconds, actualTimestampRow.getField(0));
        assertEquals(nanos, actualTimestampRow.getField(1));
    }

    @Test
    public void shouldAddExtraFieldsToRow() {
        SimpleGroupDeserializer simpleGroupDeserializer = new SimpleGroupDeserializer(TestPrimitiveMessage.class.getTypeName(), 9, "rowtime", stencilClientOrchestrator);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("is_valid")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_number")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_hash")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("latitude")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("longitude")
                .required(PrimitiveType.PrimitiveTypeName.FLOAT).named("price")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("packet_count")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("phone")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("event_timestamp")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("service_type")
                .named("TestGroupType");

        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        long currentTimeInMillis = Instant.now().toEpochMilli();
        simpleGroup.add("event_timestamp", currentTimeInMillis);
        int expectedRowCount = TestPrimitiveMessage.getDescriptor().getFields().size() + 2;

        Row row = simpleGroupDeserializer.deserialize(simpleGroup);

        assertEquals(expectedRowCount, row.getArity());
        assertEquals(true, row.getField(row.getArity() - 2));
        assertEquals(Timestamp.from(Instant.ofEpochMilli(currentTimeInMillis)), row.getField(row.getArity() - 1));
    }

    @Test
    public void shouldSetDefaultValueForAllPrimitiveTypeFieldsExceptTimestampIfMissingInSimpleGroup() {
        SimpleGroupDeserializer simpleGroupDeserializer = new SimpleGroupDeserializer(TestPrimitiveMessage.class.getTypeName(), 9, "rowtime", stencilClientOrchestrator);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("is_valid")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_number")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_hash")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("latitude")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("longitude")
                .required(PrimitiveType.PrimitiveTypeName.FLOAT).named("price")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("packet_count")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("phone")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("event_timestamp")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("service_type")
                .named("TestGroupType");

        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);
        long currentTimeInMillis = Instant.now().toEpochMilli();
        simpleGroup.add("event_timestamp", currentTimeInMillis);

        Row row = simpleGroupDeserializer.deserialize(simpleGroup);

        assertEquals(false, row.getField(getProtoIndex("is_valid")));
        assertEquals("", row.getField(getProtoIndex("order_number")));
        assertNull(row.getField(getProtoIndex("order_hash")));
        assertEquals(0.0D, row.getField(getProtoIndex("latitude")));
        assertEquals(0.0F, row.getField(getProtoIndex("price")));
        assertEquals(0, row.getField(getProtoIndex("packet_count")));
        assertEquals(0L, row.getField(getProtoIndex("phone")));
        assertEquals("UNKNOWN", row.getField(getProtoIndex("service_type")));
    }

    @Test
    public void shouldThrowExceptionIfTimestampIsMissingInSimpleGroup() {
        SimpleGroupDeserializer simpleGroupDeserializer = new SimpleGroupDeserializer(TestPrimitiveMessage.class.getTypeName(), 9, "rowtime", stencilClientOrchestrator);
        GroupType parquetSchema = org.apache.parquet.schema.Types.requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("is_valid")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_number")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("order_hash")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("latitude")
                .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("longitude")
                .required(PrimitiveType.PrimitiveTypeName.FLOAT).named("price")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("packet_count")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("phone")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("event_timestamp")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("service_type")
                .named("TestGroupType");

        SimpleGroup simpleGroup = new SimpleGroup(parquetSchema);

        DaggerDeserializationException exception = Assert.assertThrows(DaggerDeserializationException.class,
                () -> simpleGroupDeserializer.deserialize(simpleGroup));
        assertEquals("io.odpf.dagger.common.exceptions.serde.InvalidDataTypeException: Could not extract " +
                "timestamp with field name event_timestamp from simple group of type required group TestGroupType {\n" +
                "  required boolean is_valid;\n" +
                "  required binary order_number;\n" +
                "  required binary order_hash;\n" +
                "  required double latitude;\n" +
                "  required double longitude;\n" +
                "  required float price;\n" +
                "  required int32 packet_count;\n" +
                "  required int64 phone;\n" +
                "  required int64 event_timestamp;\n" +
                "  required binary service_type;\n" +
                "}", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfSimpleGroupIsNull() {
        SimpleGroupDeserializer simpleGroupDeserializer = new SimpleGroupDeserializer(TestPrimitiveMessage.class.getTypeName(), 9, "rowtime", stencilClientOrchestrator);

        DaggerDeserializationException exception = Assert.assertThrows(DaggerDeserializationException.class,
                () -> simpleGroupDeserializer.deserialize(null));
        assertEquals("java.lang.IllegalArgumentException: Error: Cannot parse timestamp from null", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfDescriptorIsNotFound() {
        assertThrows(DescriptorNotFoundException.class,
                () -> new SimpleGroupDeserializer(String.class.getTypeName(), 6, "rowtime", stencilClientOrchestrator));
    }

    private int getProtoIndex(String propertyName) {
        return TestPrimitiveMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }
}