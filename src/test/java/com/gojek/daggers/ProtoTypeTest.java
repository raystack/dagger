package com.gojek.daggers;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
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
            new ProtoType("com.gojek.daggers.ProtoType");
            fail();
        } catch (DaggerConfigurationException exception) {
            assertEquals(ProtoType.PROTO_CLASS_MISCONFIGURED_ERROR, exception.getMessage());
            assertTrue(exception.getCause() instanceof ReflectiveOperationException);
        }
    }

    @Test
    public void shouldGiveMappedFlinkTypes(){

        ProtoType participantKeyProtoType = new ProtoType("com.gojek.esb.participant.ParticipantLogKey");
        ProtoType participantMessageProtoType = new ProtoType("com.gojek.esb.participant.ParticipantLogMessage");

        assertArrayEquals(
                new TypeInformation[]{Types.STRING(), Types.STRING(), Types.ROW(), Types.STRING(), Types.STRING(), Types.STRING(), Types.ROW()}
                , participantKeyProtoType.getFieldTypes());

        assertArrayEquals(
                new TypeInformation[]{
                        Types.STRING(),Types.STRING(), Types.ROW(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(),
                        Types.ROW(),Types.INT(),Types.DOUBLE(),Types.INT(),Types.DOUBLE(),Types.ROW(),Types.ROW(), Types.STRING(),
                        Types.STRING(),Types.STRING()
                }
                , participantMessageProtoType.getFieldTypes());

    }
}