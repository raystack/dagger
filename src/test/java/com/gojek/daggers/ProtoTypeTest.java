package com.gojek.daggers;

import com.gojek.esb.participant.ParticipantLogMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProtoTypeTest {

    @Test
    public void shouldGiveAllColumnNamesOfProto() throws ClassNotFoundException {
        ProtoType participantKeyProtoType = new ProtoType("com.gojek.esb.participant.ParticipantLogKey");
        ProtoType bookingKeyProtoType = new ProtoType("com.gojek.esb.booking.BookingLogKey");

        assertArrayEquals(
                new String[]{"order_id", "status", "event_timestamp", "bid_id", "service_type", "participant_id", "audit"}
                , participantKeyProtoType.getFieldNames());

        assertArrayEquals(
                new String[]{"service_type", "order_number", "order_url", "status", "event_timestamp", "audit"}
                , bookingKeyProtoType.getFieldNames());
    }

    @Test
    public void shouldThrowConfigurationExceptionWhenClassNotFound() {
        try {
            new ProtoType("com.gojek.esb.participant.ParticipantLogKey211");
            fail();
        } catch (DaggerConfigurationException exception) {
            assertEquals(ProtoType.PROTO_CLASS_MISCONFIGURED_ERROR, exception.getMessage());
            assertTrue(exception.getCause() instanceof ReflectiveOperationException);
        }
    }

    @Test
    public void shouldThrowConfigurationExceptionWhenClassIsNotProto() {
        try {
            new ProtoType(String.class.getName());
            fail();
        } catch (DaggerConfigurationException exception) {
            assertEquals(ProtoType.PROTO_CLASS_MISCONFIGURED_ERROR, exception.getMessage());
            assertTrue(exception.getCause() instanceof ReflectiveOperationException);
        }
    }

    @Test
    public void shouldGiveSimpleMappedFlinkTypes(){
        ProtoType participantMessageProtoType = new ProtoType(ParticipantLogMessage.class.getName());

        TypeInformation[] fieldTypes = participantMessageProtoType.getFieldTypes();

        assertEquals(Types.STRING(), fieldTypes[participantLogFieldIndex("order_id")]);
        assertEquals(Types.INT(), fieldTypes[participantLogFieldIndex("customer_straight_line_distance")]);
        assertEquals(Types.DOUBLE(), fieldTypes[participantLogFieldIndex("receive_delay")]);
    }

    @Test
    public void shouldGiveSubRowMappedField(){
        ProtoType participantMessageProtoType = new ProtoType(ParticipantLogMessage.class.getName());

        TypeInformation[] fieldTypes = participantMessageProtoType.getFieldTypes();

        TypeInformation<Row> expectedTimestampRow = Types.ROW(new String[]{"seconds", "nanos"},
                                                                new TypeInformation<?>[]{Types.LONG(), Types.INT()});
        TypeInformation<Row> driverLocationRow = Types.ROW(new String[]{"latitude", "longitude", "altitude", "accuracy", "speed"},
                new TypeInformation<?>[]{Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()});

        assertEquals(expectedTimestampRow, fieldTypes[participantLogFieldIndex("event_timestamp")]);
        assertEquals(driverLocationRow, fieldTypes[participantLogFieldIndex("location")]);
    }

    private int participantLogFieldIndex(String propertyName) {
        return ParticipantLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }
}