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
        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100l).setNanos(111000).build();
        ParticipantLogKey proto = ParticipantLogKey.newBuilder().setEventTimestamp(expectedTimestamp).build();
        Row protoRow = new ProtoDeserializer(ParticipantLogKey.class.getName(), new ProtoType(ParticipantLogKey.class.getName())).deserialize(proto.toByteArray());
        int timestampRowIndex = ParticipantLogKey.getDescriptor().findFieldByName("event_timestamp").getIndex();

        RowTimestampExtractor rowTimestampExtractor = new RowTimestampExtractor(timestampRowIndex);
        long actualTimestamp = rowTimestampExtractor.extractTimestamp(protoRow, 0l);

        assertEquals(epochMilliSecondTime(expectedTimestamp), actualTimestamp);
    }

    private long epochMilliSecondTime(Timestamp expectedTimestamp) {
        return expectedTimestamp.getSeconds() * 1000 + expectedTimestamp.getNanos()/1000;
    }

    @Test
    public void shouldGiveBackCurrentTimestampAsWatermark() throws IOException{

        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100l).setNanos(111).build();
        ParticipantLogKey proto = ParticipantLogKey.newBuilder().setEventTimestamp(expectedTimestamp).build();
        Row protoRow = new ProtoDeserializer(ParticipantLogKey.class.getName(), new ProtoType(ParticipantLogKey.class.getName())).deserialize(proto.toByteArray());
        int timestampRowIndex = ParticipantLogKey.getDescriptor().findFieldByName("event_timestamp").getIndex();
        RowTimestampExtractor rowTimestampExtractor = new RowTimestampExtractor(timestampRowIndex);
        rowTimestampExtractor.extractTimestamp(protoRow, 0l);

        assertEquals(epochMilliSecondTime(expectedTimestamp), rowTimestampExtractor.getCurrentWatermark().getTimestamp());
    }
}
