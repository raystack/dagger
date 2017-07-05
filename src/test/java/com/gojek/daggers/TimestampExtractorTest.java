package com.gojek.daggers;

import com.gojek.esb.participant.ParticipantLogKey;
import com.gojek.esb.participant.ParticipantLogMessage;
import com.google.protobuf.Timestamp;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TimestampExtractorTest {

    @Test
    public void shouldRetrieveTimestampBasedOnRowIndex() throws IOException {
        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100l).setNanos(111).build();
        ParticipantLogKey proto = ParticipantLogKey.newBuilder().setEventTimestamp(expectedTimestamp).build();
        Row protoRow = new ProtoDeserializer(ParticipantLogKey.class.getName(), new ProtoType(ParticipantLogKey.class.getName())).deserialize(proto.toByteArray());
        int timestampRowIndex = ParticipantLogKey.getDescriptor().findFieldByName("event_timestamp").getIndex();

        TimestampExtractor timestampExtractor = new TimestampExtractor(timestampRowIndex);
        long actualTimestamp = timestampExtractor.extractTimestamp(protoRow, 0l);

        assertEquals(epochTime(expectedTimestamp), actualTimestamp);
    }

    private long epochTime(Timestamp expectedTimestamp) {
        return expectedTimestamp.getSeconds() + expectedTimestamp.getNanos()/(1000*1000);
    }

    @Test
    public void shouldGiveBackCurrentTimestampAsWatermark() throws IOException{

        Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(100l).setNanos(111).build();
        ParticipantLogKey proto = ParticipantLogKey.newBuilder().setEventTimestamp(expectedTimestamp).build();
        Row protoRow = new ProtoDeserializer(ParticipantLogKey.class.getName(), new ProtoType(ParticipantLogKey.class.getName())).deserialize(proto.toByteArray());
        int timestampRowIndex = ParticipantLogKey.getDescriptor().findFieldByName("event_timestamp").getIndex();
        TimestampExtractor timestampExtractor = new TimestampExtractor(timestampRowIndex);
        timestampExtractor.extractTimestamp(protoRow, 0l);

        assertEquals(epochTime(expectedTimestamp), timestampExtractor.getCurrentWatermark().getTimestamp());
    }
}
