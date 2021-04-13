package io.odpf.dagger.source;

import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.exception.DescriptorNotFoundException;
import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.clevertap.ClevertapEventLogMessage;
import com.gojek.esb.consumer.TestNestedRepeatedMessage;
import com.gojek.esb.login.LoginRequestMessage;
import com.gojek.esb.participant.ParticipantLogMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.utils.Constants.*;
import static org.apache.flink.api.common.typeinfo.Types.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProtoTypeTest {

    @Mock
    public Configuration configuration;

    private StencilClientOrchestrator stencilClientOrchestrator;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getString(STENCIL_CONFIG_REFRESH_CACHE_KEY, STENCIL_CONFIG_REFRESH_CACHE_DEFAULT)).thenReturn(STENCIL_CONFIG_REFRESH_CACHE_DEFAULT);
        when(configuration.getString(STENCIL_CONFIG_TTL_IN_MINUTES_KEY, STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT)).thenReturn(STENCIL_CONFIG_TTL_IN_MINUTES_DEFAULT);
        when(configuration.getBoolean(STENCIL_ENABLE_KEY, STENCIL_ENABLE_DEFAULT)).thenReturn(STENCIL_ENABLE_DEFAULT);
        when(configuration.getString(STENCIL_URL_KEY, STENCIL_URL_DEFAULT)).thenReturn(STENCIL_URL_DEFAULT);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);
    }


    @Test
    public void shouldGiveAllColumnNamesOfProtoAlongWithRowtime() {
        ProtoType participantKeyProtoType = new ProtoType("com.gojek.esb.participant.ParticipantLogKey", "rowtime", stencilClientOrchestrator);
        ProtoType bookingKeyProtoType = new ProtoType("com.gojek.esb.booking.BookingLogKey", "rowtime", stencilClientOrchestrator);

        assertArrayEquals(
                new String[]{"order_id", "status", "event_timestamp", "bid_id", "service_type", "participant_id", "audit", INTERNAL_VALIDATION_FILED, "rowtime"},
                ((RowTypeInfo) participantKeyProtoType.getRowType()).getFieldNames());

        assertArrayEquals(
                new String[]{"service_type", "order_number", "order_url", "status", "event_timestamp", "audit", INTERNAL_VALIDATION_FILED,"rowtime"},
                ((RowTypeInfo) bookingKeyProtoType.getRowType()).getFieldNames());
    }

    @Test
    public void shouldGiveAllTypesOfFieldsAlongWithRowtime() {
        ProtoType participantKeyProtoType = new ProtoType("com.gojek.esb.participant.ParticipantLogKey", "rowtime", stencilClientOrchestrator);

        assertArrayEquals(
                new TypeInformation[]{STRING, STRING, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT), STRING, STRING, STRING, ROW_NAMED(new String[]{"request_id", "timestamp"}, STRING, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT)), BOOLEAN, SQL_TIMESTAMP},
                ((RowTypeInfo) participantKeyProtoType.getRowType()).getFieldTypes());
    }

    @Test(expected = DescriptorNotFoundException.class)
    public void shouldThrowConfigurationExceptionWhenClassNotFound() {
        ProtoType protoType = new ProtoType("com.gojek.esb.participant.ParticipantLogKey211", "rowtime", stencilClientOrchestrator);
        protoType.getRowType();
    }

    @Test(expected = DescriptorNotFoundException.class)
    public void shouldThrowConfigurationExceptionWhenClassIsNotProto() {
        ProtoType protoType = new ProtoType(String.class.getName(), "rowtime", stencilClientOrchestrator);
        protoType.getRowType();
    }

    @Test
    public void shouldGiveSimpleMappedFlinkTypes() {
        ProtoType participantMessageProtoType = new ProtoType(ParticipantLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        TypeInformation[] fieldTypes = ((RowTypeInfo) participantMessageProtoType.getRowType()).getFieldTypes();

        assertEquals(STRING, fieldTypes[participantLogFieldIndex("order_id")]);
        assertEquals(INT, fieldTypes[participantLogFieldIndex("customer_straight_line_distance")]);
        assertEquals(DOUBLE, fieldTypes[participantLogFieldIndex("receive_delay")]);
    }

    @Test
    public void shouldGiveSubRowMappedField() {
        ProtoType participantMessageProtoType = new ProtoType(ParticipantLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        TypeInformation[] fieldTypes = ((RowTypeInfo) participantMessageProtoType.getRowType()).getFieldTypes();

        TypeInformation<Row> expectedTimestampRow = ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT);

        TypeInformation<Row> driverLocationRow = ROW_NAMED(new String[]{"latitude", "longitude", "altitude", "accuracy", "speed", "event_timestamp"},
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT));

        assertEquals(expectedTimestampRow, fieldTypes[participantLogFieldIndex("event_timestamp")]);
        assertEquals(driverLocationRow, fieldTypes[participantLogFieldIndex("location")]);
    }

    @Test
    public void shouldProcessArrayForObjectData() {
        ProtoType bookingLogMessageProtoType = new ProtoType(BookingLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        TypeInformation[] fieldTypes = ((RowTypeInfo) bookingLogMessageProtoType.getRowType()).getFieldTypes();
        TypeInformation<Row> locationType = ROW_NAMED(new String[]{"name", "address", "latitude", "longitude", "type", "note", "place_id", "accuracy_meter", "gate_id"},
                STRING, STRING, DOUBLE, DOUBLE, STRING, STRING, STRING, FLOAT, STRING);
        TypeInformation<?> expectedRoutesRow = OBJECT_ARRAY(ROW_NAMED(new String[]{"startTimer", "end", "distance_in_kms", "estimated_duration", "route_order"},
                locationType, locationType, FLOAT, ROW_NAMED(new String[]{"seconds", "nanos"}, LONG, INT), INT));

        assertEquals(expectedRoutesRow, fieldTypes[bookingLogFieldIndex("routes")]);
    }

    @Test
    public void shouldProcessArrayForStringData() {
        ProtoType loginRequestMessageProtoType = new ProtoType(LoginRequestMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        TypeInformation[] fieldTypes = ((RowTypeInfo) loginRequestMessageProtoType.getRowType()).getFieldTypes();

        TypeInformation<?> registeredDeviceType = ObjectArrayTypeInfo.getInfoFor(STRING);

        assertEquals(registeredDeviceType, fieldTypes[loginRequestFieldIndex("registered_device")]);
    }


    @Test
    public void shouldGiveNamesAndTypes() {
        ProtoType participantMessageProtoType = new ProtoType(ParticipantLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);

        RowTypeInfo rowType = (RowTypeInfo) participantMessageProtoType.getRowType();

        assertEquals("order_id", rowType.getFieldNames()[0]);
        assertEquals(DOUBLE, rowType.getFieldTypes()[participantLogFieldIndex("receive_delay")]);
    }

    @Test
    public void shouldGiveAllNamesAndTypesIncludingStructFields() {
        ProtoType clevertapMessageProtoType = new ProtoType(ClevertapEventLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);
        assertEquals(12, ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldNames().length);
        assertEquals(12, ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldTypes().length);
    }

    @Test
    public void shouldReturnRowTypeForStructFields() {
        ProtoType clevertapMessageProtoType = new ProtoType(ClevertapEventLogMessage.class.getName(), "rowtime", stencilClientOrchestrator);
        assertEquals(ROW(), ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldTypes()[7]);
        assertEquals(ROW(), ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldTypes()[8]);
        assertEquals(ROW(), ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldTypes()[9]);
        assertEquals("profile_data", ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldNames()[7]);
        assertEquals("event_properties", ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldNames()[8]);
        assertEquals("key_values", ((RowTypeInfo) clevertapMessageProtoType.getRowType()).getFieldNames()[9]);
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

    private int participantLogFieldIndex(String propertyName) {
        return ParticipantLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

    private int bookingLogFieldIndex(String propertyName) {
        return BookingLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

    private int loginRequestFieldIndex(String propertyName) {
        return LoginRequestMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }

}
