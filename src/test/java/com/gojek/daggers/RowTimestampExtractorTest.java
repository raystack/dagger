package com.gojek.daggers;

import com.gojek.esb.participant.ParticipantLogKey;
import com.google.protobuf.Timestamp;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class RowTimestampExtractorTest {

    @Test
    public void shouldRetrieveTimestampBasedOnRowIndex() throws IOException {
        final int expectedSeconds = 100;
        final int expectedMilliseconds = 111000;
        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(expectedSeconds).setNanos(expectedMilliseconds).build();
        ParticipantLogKey proto = ParticipantLogKey.newBuilder().setEventTimestamp(expectedTimestamp).build();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogKey.class.getName(), new ProtoType(ParticipantLogKey.class.getName()));
        Row protoRow = protoDeserializer.deserialize(null, proto.toByteArray(), null, 0, 0);
        int timestampRowIndex = ParticipantLogKey.getDescriptor().findFieldByName("event_timestamp").getIndex();

        RowTimestampExtractor rowTimestampExtractor = new RowTimestampExtractor(timestampRowIndex);
        long actualTimestamp = rowTimestampExtractor.extractTimestamp(protoRow, 0L);

        assertEquals(epochMilliSecondTime(expectedTimestamp), actualTimestamp);
    }

    private long epochMilliSecondTime(Timestamp expectedTimestamp) {
        final int millisecondFactor = 1000;
        return expectedTimestamp.getSeconds() * millisecondFactor + expectedTimestamp.getNanos() / millisecondFactor;
    }

    @Test
    public void shouldGiveBackCurrentTimestampAsWatermark() throws IOException {
        final int expectedSeconds = 100;
        final int expectedMilliseconds = 111000;
        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(expectedSeconds).setNanos(expectedMilliseconds).build();
        ParticipantLogKey proto = ParticipantLogKey.newBuilder().setEventTimestamp(expectedTimestamp).build();
        ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogKey.class.getName(), new ProtoType(ParticipantLogKey.class.getName()));
        Row protoRow = protoDeserializer.deserialize(null, proto.toByteArray(), null, 0, 0);
        int timestampRowIndex = ParticipantLogKey.getDescriptor().findFieldByName("event_timestamp").getIndex();
        RowTimestampExtractor rowTimestampExtractor = new RowTimestampExtractor(timestampRowIndex);
        rowTimestampExtractor.extractTimestamp(protoRow, 0L);

        assertEquals(epochMilliSecondTime(expectedTimestamp), rowTimestampExtractor.getCurrentWatermark().getTimestamp());
    }
}
