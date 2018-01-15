package com.gojek.daggers;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class KafkaProtoStreamingTableSourceTest {

  @Mock
  private StreamExecutionEnvironment streamEnv;

  @Mock
  private SingleOutputStreamOperator dataStream;

  @Mock
  private DataStreamSource dataStreamSource;

  @Mock
  private FlinkKafkaConsumerBase flinkConsumer;

  @Mock
  private ProtoType protoType;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void shouldAssignInjectedTimestampExtractor() {
    when(streamEnv.addSource(any())).thenReturn(dataStreamSource);
    when(dataStreamSource.assignTimestampsAndWatermarks(any(AssignerWithPeriodicWatermarks.class))).thenReturn(dataStream);
    RowTimestampExtractor expectedRowTimestampExtractor = new RowTimestampExtractor(1, new Configuration());

    DataStream<Row> actualDataStream = new KafkaProtoStreamingTableSource(flinkConsumer, expectedRowTimestampExtractor, "window_timestamp").getDataStream(streamEnv);

    assertEquals(dataStream, actualDataStream);
    verify(streamEnv).addSource(flinkConsumer);
    verify(dataStreamSource).assignTimestampsAndWatermarks(expectedRowTimestampExtractor);
  }

  @Test
  public void shouldUseInjectedNameForRowtimeAttribute() {
    String expectedRowTimeAttribute = "window_timestamp";
    assertEquals(expectedRowTimeAttribute, new KafkaProtoStreamingTableSource(null, null, "window_timestamp").getRowtimeAttribute());
  }

  @Test
  public void shouldGiveBackRowTypesComingFromProtoType() {
    RowTypeInfo expectedRowType = new RowTypeInfo();
    when(flinkConsumer.getProducedType()).thenReturn(expectedRowType);
    KafkaProtoStreamingTableSource kafkaProtoStreamingTableSource = new KafkaProtoStreamingTableSource(flinkConsumer, null, "");

    assertEquals(expectedRowType, kafkaProtoStreamingTableSource.getReturnType());
  }
}
