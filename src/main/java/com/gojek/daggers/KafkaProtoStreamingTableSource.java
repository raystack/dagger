package com.gojek.daggers;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.sources.DefinedRowtimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

public class KafkaProtoStreamingTableSource implements StreamTableSource<Row>, DefinedRowtimeAttribute {

  private final FlinkKafkaConsumerBase<Row> kafkaConsumer;
  private final RowTimestampExtractor rowTimestampExtractor;
  private String rowTimeAttributeName;

  public KafkaProtoStreamingTableSource(FlinkKafkaConsumerBase<Row> kafkaConsumer, RowTimestampExtractor rowTimestampExtractor, String rowTimeAttributeName) {

    this.kafkaConsumer = kafkaConsumer;
    this.rowTimestampExtractor = rowTimestampExtractor;
    this.rowTimeAttributeName = rowTimeAttributeName;
  }

  @Override
  public TypeInformation<Row> getReturnType() {
    return kafkaConsumer.getProducedType();
  }

  @Override
  public String explainSource() {
    return "Lets you write sql queries for streaming protobuf data from kafka!!";
  }

  @Override
  public String getRowtimeAttribute() {
    return rowTimeAttributeName;
  }

  @Override
  public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
    return execEnv.addSource(kafkaConsumer).assignTimestampsAndWatermarks(rowTimestampExtractor);
  }

}
