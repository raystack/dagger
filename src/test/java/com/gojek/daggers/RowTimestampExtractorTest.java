package com.gojek.daggers;

import com.gojek.esb.participant.ParticipantLogKey;
import com.google.protobuf.Timestamp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class RowTimestampExtractorTest {

  private Configuration config;

  @Before
  public void setup() {
    config = new Configuration();
  }

  @Test
  public void shouldRetrieveTimestampBasedOnRowIndex() throws IOException {
    final int expectedSeconds = 100;
    final int expectedNanos = 111000000;
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(expectedSeconds).setNanos(expectedNanos).build();
    ParticipantLogKey proto = ParticipantLogKey.newBuilder().setEventTimestamp(expectedTimestamp).build();
    ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogKey.class.getName(), new ProtoType(ParticipantLogKey.class.getName()));
    Row protoRow = protoDeserializer.deserialize(null, proto.toByteArray(), null, 0, 0);
    int timestampRowIndex = ParticipantLogKey.getDescriptor().findFieldByName("event_timestamp").getIndex();

    RowTimestampExtractor rowTimestampExtractor = new RowTimestampExtractor(timestampRowIndex, config);
    long actualTimestamp = rowTimestampExtractor.extractTimestamp(protoRow, 0L);

    assertEquals(epochMilliSecondTime(expectedTimestamp), actualTimestamp);
  }

  private long epochMilliSecondTime(Timestamp expectedTimestamp) {
    return Instant.ofEpochSecond(expectedTimestamp.getSeconds(), expectedTimestamp.getNanos()).toEpochMilli();
  }

  @Test
  public void shouldGiveBackCurrentTimestampAsWatermark() throws IOException {
    final int watermarkTimeDifferenceInSeconds = 1800;
    final int actualTimestampSeconds = 10000;
    final int expectedSeconds = actualTimestampSeconds - watermarkTimeDifferenceInSeconds;
    final int expectedMilliseconds = 111000;
    final int milisecondFactor = 1000;
    config.setInteger("WATERMARK_DELAY_MS", watermarkTimeDifferenceInSeconds * milisecondFactor);

    Timestamp actualTimestamp = Timestamp.newBuilder().setSeconds(actualTimestampSeconds).setNanos(expectedMilliseconds).build();
    Timestamp expectedTimestamp = Timestamp.newBuilder().setSeconds(expectedSeconds).setNanos(expectedMilliseconds).build();
    ParticipantLogKey proto = ParticipantLogKey.newBuilder().setEventTimestamp(actualTimestamp).build();
    ProtoDeserializer protoDeserializer = new ProtoDeserializer(ParticipantLogKey.class.getName(), new ProtoType(ParticipantLogKey.class.getName()));
    Row protoRow = protoDeserializer.deserialize(null, proto.toByteArray(), null, 0, 0);
    int timestampRowIndex = ParticipantLogKey.getDescriptor().findFieldByName("event_timestamp").getIndex();

    RowTimestampExtractor rowTimestampExtractor = new RowTimestampExtractor(timestampRowIndex, config);
    rowTimestampExtractor.extractTimestamp(protoRow, 0L);

    assertEquals(epochMilliSecondTime(expectedTimestamp), rowTimestampExtractor.getCurrentWatermark().getTimestamp());
  }
}
