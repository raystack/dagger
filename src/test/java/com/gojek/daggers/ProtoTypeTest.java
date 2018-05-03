package com.gojek.daggers;

import com.gojek.esb.booking.BookingLogMessage;
import com.gojek.esb.login.LoginRequestMessage;
import com.gojek.esb.participant.ParticipantLogMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class ProtoTypeTest {

    @Before
    public void before() {
        DescriptorStore.load(new Configuration());
    }
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void shouldGiveAllColumnNamesOfProto() throws ClassNotFoundException {
        ProtoType participantKeyProtoType = new ProtoType("com.gojek.esb.participant.ParticipantLogKey", "rowtime");
        ProtoType bookingKeyProtoType = new ProtoType("com.gojek.esb.booking.BookingLogKey", "rowtime");

        assertArrayEquals(
                new String[]{"order_id", "status", "event_timestamp", "bid_id", "service_type", "participant_id", "audit"},
                participantKeyProtoType.getFieldNames());

        assertArrayEquals(
                new String[]{"service_type", "order_number", "order_url", "status", "event_timestamp", "audit"},
                bookingKeyProtoType.getFieldNames());
    }

    @Test
    public void shouldThrowConfigurationExceptionWhenClassNotFound() {
        expectedEx.expect(DaggerConfigurationException.class);
        expectedEx.expectMessage(DescriptorStore.PROTO_CLASS_MISCONFIGURED_ERROR);
        new ProtoType("com.gojek.esb.participant.ParticipantLogKey211", "rowtime");
    }

    @Test
    public void shouldThrowConfigurationExceptionWhenClassIsNotProto() {
        expectedEx.expect(DaggerConfigurationException.class);
        expectedEx.expectMessage(DescriptorStore.PROTO_CLASS_MISCONFIGURED_ERROR);
        new ProtoType(String.class.getName(), "rowtime");

    }

    @Test
    public void shouldGiveSimpleMappedFlinkTypes() {
        ProtoType participantMessageProtoType = new ProtoType(ParticipantLogMessage.class.getName(), "rowtime");

        TypeInformation[] fieldTypes = participantMessageProtoType.getFieldTypes();

        assertEquals(Types.STRING(), fieldTypes[participantLogFieldIndex("order_id")]);
        assertEquals(Types.INT(), fieldTypes[participantLogFieldIndex("customer_straight_line_distance")]);
        assertEquals(Types.DOUBLE(), fieldTypes[participantLogFieldIndex("receive_delay")]);
    }

    @Test
    public void shouldGiveSubRowMappedField() {
        ProtoType participantMessageProtoType = new ProtoType(ParticipantLogMessage.class.getName(), "rowtime");

        TypeInformation[] fieldTypes = participantMessageProtoType.getFieldTypes();

        TypeInformation<Row> expectedTimestampRow = Types.ROW(new String[]{"seconds", "nanos"},
                new TypeInformation<?>[]{Types.LONG(), Types.INT()});
        TypeInformation<Row> driverLocationRow = Types.ROW(new String[]{"latitude", "longitude", "altitude", "accuracy", "speed"},
                new TypeInformation<?>[]{Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()});

        assertEquals(expectedTimestampRow, fieldTypes[participantLogFieldIndex("event_timestamp")]);
        assertEquals(driverLocationRow, fieldTypes[participantLogFieldIndex("location")]);
    }

    @Test
    public void shouldProcessArrayForObjectData() {
        ProtoType bookingLogMessageProtoType = new ProtoType(BookingLogMessage.class.getName(), "rowtime");

        TypeInformation[] fieldTypes = bookingLogMessageProtoType.getFieldTypes();

        TypeInformation<Row> locationType = Types.ROW(new String[]{"name", "address", "latitude", "longitude", "type",
                "note", "place_id"}, new TypeInformation<?>[]{Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(),
                Types.STRING(), Types.STRING(), Types.STRING()});
        TypeInformation<?> expectedRoutesRow = Types.OBJECT_ARRAY(Types.ROW(new String[]{"start", "end",
                "distance_in_kms", "estimated_duration", "route_order"}, new TypeInformation<?>[]{locationType, locationType,
                Types.FLOAT(), Types.ROW(new String[]{"seconds", "nanos"}, new TypeInformation<?>[]{Types.LONG(),
                Types.INT()}), Types.INT()}));

        assertEquals(expectedRoutesRow, fieldTypes[bookingLogFieldIndex("routes")]);
    }

    @Test
    public void shouldProcessArrayForStringData() {
        ProtoType loginRequestMessageProtoType = new ProtoType(LoginRequestMessage.class.getName(), "rowtime");

        TypeInformation[] fieldTypes = loginRequestMessageProtoType.getFieldTypes();

        TypeInformation<?> registeredDeviceType = Types.OBJECT_ARRAY(Types.STRING());

        assertEquals(registeredDeviceType, fieldTypes[loginRequestFieldIndex("registered_device")]);
    }

    //Add test for array of primitive data type. Couldn't find a proto which uses array of primitive data type.

    @Test
    public void shouldGiveNamesAndTypes() {
        ProtoType participantMessageProtoType = new ProtoType(ParticipantLogMessage.class.getName(), "rowtime");

        RowTypeInfo rowType = (RowTypeInfo) participantMessageProtoType.getRowType();

        assertEquals("order_id", rowType.getFieldNames()[0]);
        assertEquals(Types.DOUBLE(), rowType.getFieldTypes()[participantLogFieldIndex("receive_delay")]);

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
