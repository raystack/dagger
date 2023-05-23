package com.gotocompany.dagger.common.serde.proto.deserialization;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.common.exceptions.DescriptorNotFoundException;
import com.gotocompany.dagger.consumer.TestBookingLogMessage;
import com.gotocompany.dagger.consumer.TestNestedRepeatedMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.gotocompany.dagger.common.core.Constants.*;
import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoTypeTest {

    @Mock
    private Configuration configuration;

    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_ENABLE_KEY, SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_URLS_KEY, SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT);
        when(configuration.getBoolean(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH_DEFAULT);
        when(configuration.getLong(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_KEY, SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS_DEFAULT);
        when(configuration.getString(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_KEY, SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT)).thenReturn(SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY_DEFAULT);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }


    @Test
    public void shouldGiveAllColumnNamesOfProtoAlongWithRowtime() {
        ProtoType feedbackKeyProtoType = new ProtoType("com.gotocompany.dagger.consumer.TestFeedbackLogKey", "rowtime", stencilClientOrchestrator);
        ProtoType bookingKeyProtoType = new ProtoType("com.gotocompany.dagger.consumer.TestBookingLogKey", "rowtime", stencilClientOrchestrator);

        assertArrayEquals(
                new String[]{"order_number", "event_timestamp", INTERNAL_VALIDATION_FIELD_KEY, "rowtime"},
                ((RowTypeInfo) feedbackKeyProtoType.getRowType()).getFieldNames());

        assertArrayEquals(
                new String[]{"service_type", "order_number", "order_url", "status", "event_timestamp", INTERNAL_VALIDATION_FIELD_KEY, "rowtime"},
                ((RowTypeInfo) bookingKeyProtoType.getRowType()).getFieldNames());
    }

    @Test
    public void shouldGiveAllTypesOfFieldsAlongWithRowtime() {
        ProtoType protoType = new ProtoType("com.gotocompany.dagger.consumer.TestBookingLogKey", "rowtime", stencilClientOrchestrator);

        assertArrayEquals(
                new TypeInformation[]{STRING, STRING, STRING, STRING, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT), BOOLEAN, SQL_TIMESTAMP},
                ((RowTypeInfo) protoType.getRowType()).getFieldTypes());
    }

    @Test
    public void shouldThrowConfigurationExceptionWhenClassNotFound() {
        ProtoType protoType = new ProtoType("com.gotocompany.dagger.consumer.NotFoundClass", "rowtime", stencilClientOrchestrator);
        assertThrows(DescriptorNotFoundException.class,
                () -> protoType.getRowType());
    }

    @Test
    public void shouldThrowConfigurationExceptionWhenClassIsNotProto() {
        ProtoType protoType = new ProtoType(String.class.getName(), "rowtime", stencilClientOrchestrator);
        assertThrows(DescriptorNotFoundException.class,
                () -> protoType.getRowType());

    }

    @Test
    public void shouldGiveSimpleMappedFlinkTypes() {
        ProtoType protoType = new ProtoType(TestBookingLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        TypeInformation[] fieldTypes = ((RowTypeInfo) protoType.getRowType()).getFieldTypes();

        assertEquals(STRING, fieldTypes[bookingLogFieldIndex("order_number")]);
        assertEquals(INT, fieldTypes[bookingLogFieldIndex("cancel_reason_id")]);
        assertEquals(DOUBLE, fieldTypes[bookingLogFieldIndex("cash_amount")]);
    }

    @Test
    public void shouldGiveSubRowMappedField() {
        ProtoType participantMessageProtoType = new ProtoType(TestBookingLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        TypeInformation[] fieldTypes = ((RowTypeInfo) participantMessageProtoType.getRowType()).getFieldTypes();

        TypeInformation<Row> expectedTimestampRow = ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT);

        TypeInformation<Row> driverLocationRow = ROW_NAMED(new String[]{"name", "address", "latitude", "longitude", "type", "note", "place_id", "accuracy_meter", "gate_id"},
                STRING, STRING, DOUBLE, DOUBLE, STRING, STRING, STRING, FLOAT, STRING);

        assertEquals(expectedTimestampRow, fieldTypes[bookingLogFieldIndex("event_timestamp")]);
        assertEquals(driverLocationRow, fieldTypes[bookingLogFieldIndex("driver_pickup_location")]);
    }

    @Test
    public void shouldProcessArrayForObjectData() {
        ProtoType bookingLogMessageProtoType = new ProtoType(TestBookingLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        TypeInformation[] fieldTypes = ((RowTypeInfo) bookingLogMessageProtoType.getRowType()).getFieldTypes();
        TypeInformation<Row> locationType = ROW_NAMED(new String[]{"name", "address", "latitude", "longitude", "type", "note", "place_id", "accuracy_meter", "gate_id"},
                STRING, STRING, DOUBLE, DOUBLE, STRING, STRING, STRING, FLOAT, STRING);
        TypeInformation<?> expectedRoutesRow = OBJECT_ARRAY(ROW_NAMED(new String[]{"startTimer", "end", "distance_in_kms", "estimated_duration", "route_order"},
                locationType, locationType, FLOAT, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT), INT));

        assertEquals(expectedRoutesRow, fieldTypes[bookingLogFieldIndex("routes")]);
    }

    @Test
    public void shouldProcessArrayForStringData() {
        ProtoType rowtime = new ProtoType(TestBookingLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        TypeInformation[] fieldTypes = ((RowTypeInfo) rowtime.getRowType()).getFieldTypes();

        TypeInformation<?> registeredDeviceType = ObjectArrayTypeInfo.getInfoFor(STRING);

        assertEquals(registeredDeviceType, fieldTypes[bookingLogFieldIndex("meta_array")]);
    }


    @Test
    public void shouldGiveAllNamesAndTypesIncludingStructFields() {
        ProtoType clevertapMessageProtoType = new ProtoType(TestBookingLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);
        assertEquals(51, ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldNames().length);
        assertEquals(51, ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldTypes().length);
    }

    @Test
    public void shouldReturnRowTypeForStructFields() {
        ProtoType protoType = new ProtoType(TestBookingLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);
        assertEquals(ROW(), ((RowTypeInfo) protoType.getRowType()).getFieldTypes()[35]);
        assertEquals(ROW(), ((RowTypeInfo) protoType.getRowType()).getFieldTypes()[36]);
        assertEquals(ROW(), ((RowTypeInfo) protoType.getRowType()).getFieldTypes()[37]);
        assertEquals("profile_data", ((RowTypeInfo) protoType.getRowType()).getFieldNames()[35]);
        assertEquals("event_properties", ((RowTypeInfo) protoType.getRowType()).getFieldNames()[36]);
        assertEquals("key_values", ((RowTypeInfo) protoType.getRowType()).getFieldNames()[37]);
    }

    @Test
    public void shouldGiveAllNamesAndTypesIncludingPrimitiveArrayFields() {
        ProtoType testNestedRepeatedMessage = new ProtoType(TestNestedRepeatedMessage.class.getName(), "rowtime", stencilClientOrchestrator);
        assertEquals(PRIMITIVE_ARRAY(INT), ((RowTypeInfo) testNestedRepeatedMessage.getRowType()).getFieldTypes()[3]);
    }

    @Test
    public void shouldGiveNameAndTypeForRepeatingStructType() {
        ProtoType testNestedRepeatedMessage = new ProtoType(TestNestedRepeatedMessage.class.getName(), "rowtime", stencilClientOrchestrator);
        assertEquals("metadata", ((RowTypeInfo) testNestedRepeatedMessage.getRowType()).getFieldNames()[4]);
        assertEquals(OBJECT_ARRAY(ROW()), ((RowTypeInfo) testNestedRepeatedMessage.getRowType()).getFieldTypes()[4]);
    }

    private int bookingLogFieldIndex(String propertyName) {
        return TestBookingLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

}
