package com.gojek.daggers;

import com.gojek.esb.booking.BookingLogKey;
import com.gojek.esb.participant.DriverLocation;
import com.gojek.esb.participant.ParticipantLogMessage;
import com.google.protobuf.Timestamp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static com.gojek.esb.types.ParticipantStatusProto.ParticipantStatus.Enum.ACCEPTED;
import static com.gojek.esb.types.ServiceTypeProto.ServiceType.Enum.GO_AUTO;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class ProtoDeserializerTest {

    @Mock
    private ProtoType protoType;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldGetTypeInfomationFromProtoType() {

        String[] expectedFieldNames = {"field1", "field2"};
        TypeInformation<?>[] expectedTypes = {Types.DOUBLE, Types.STRING};
        RowTypeInfo expectedRowType = new RowTypeInfo();
        when(protoType.getRowType()).thenReturn(expectedRowType);

        TypeInformation<Row> actualType = new ProtoDeserializer(BookingLogKey.class.getTypeName(), protoType).getProducedType();
        assertEquals(expectedRowType, actualType);
    }

    @Test
    public void shouldAlwaysReturnFalseForEndOfStream() {
        assertFalse(new ProtoDeserializer(BookingLogKey.class.getTypeName(), protoType).isEndOfStream(null));
    }

    @Test
    public void shouldDeserializeProtoAsRowWithSimpelFields() throws IOException {

        String expectedOrderNumber = "111";
        final int expectedIterationNumber = 10;
        byte[] protoBytes = ParticipantLogMessage.newBuilder().setOrderId(expectedOrderNumber)
                .setIterationNumber(expectedIterationNumber).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), protoType);

        Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

        assertEquals(expectedOrderNumber, row.getField(participantLogFieldIndex("order_id")));
        assertEquals(expectedIterationNumber, row.getField(participantLogFieldIndex("iteration_number")));
    }

    @Test
    public void shouldDeserializeEnumAsString() throws IOException {

        byte[] protoBytes = ParticipantLogMessage.newBuilder().setServiceType(GO_AUTO).setStatus(ACCEPTED).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), protoType);

        Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

        assertEquals(GO_AUTO.toString(), row.getField(participantLogFieldIndex("service_type")));
        assertEquals(ACCEPTED.toString(), row.getField(participantLogFieldIndex("status")));
    }

    @Test
    public void shouldDeserializeNestedMessagesAsSubRows() throws IOException {
        final int expectedSeconds = 10;
        final int expectedNanoSeconds = 10;
        final int expectedAccuracy = 111;
        final int expectedLatitude = 222;
        final int accuracyFieldIndex = 3;
        final int latitudeFieldIndex = 0;
        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(expectedSeconds).setNanos(expectedNanoSeconds).build();
        DriverLocation expectedDriverLocation = DriverLocation.newBuilder().setAccuracy(expectedAccuracy).setLatitude(expectedLatitude).build();
        byte[] protoBytes = ParticipantLogMessage.newBuilder()
                .setEventTimestamp(expectedTimestamp)
                .setLocation(expectedDriverLocation).build().toByteArray();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogMessage.class.getTypeName(), protoType);

        Row row = protoDeserializer.deserialize(null, protoBytes, null, 0, 0);

        Row eventTimestampRow = (Row) row.getField(participantLogFieldIndex("event_timestamp"));
        assertEquals(expectedTimestamp.getSeconds(), eventTimestampRow.getField(0));
        assertEquals(expectedTimestamp.getNanos(), eventTimestampRow.getField(1));

        Row locationRow = (Row) row.getField(participantLogFieldIndex("location"));
        assertEquals(expectedDriverLocation.getAccuracy(), locationRow.getField(accuracyFieldIndex));
        assertEquals(expectedDriverLocation.getLatitude(), locationRow.getField(latitudeFieldIndex));
    }

    private int participantLogFieldIndex(String propertyName) {
        return ParticipantLogMessage.getDescriptor().findFieldByName(propertyName).getIndex();
    }
}
