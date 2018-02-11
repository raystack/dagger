package com.gojek.daggers;

import com.gojek.daggers.sink.InfluxDBFactoryWrapper;
import com.gojek.daggers.sink.InfluxRowSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

public class SinkFactory {
  public static SinkFunction<Row> getSinkFunction(Configuration configuration, String[] columnNames) {
    String sink = configuration.getString("SINK_TYPE", "influx");
    if (sink.equals("kafka")) {
      String outputProtoPrefix = configuration.getString("OUTPUT_PROTO_CLASS_PREFIX", "");
      String outputBrokerList = configuration.getString("OUTPUT_KAFKA_BROKER", "");
      String outputTopic = configuration.getString("OUTPUT_KAFKA_TOPIC", "");

      ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoPrefix, columnNames);

      // Use kafka partitioner
      FlinkKafkaPartitioner partitioner = null;

      return new FlinkKafkaProducer010<Row>(outputTopic,
              protoSerializer,
              FlinkKafkaProducerBase.getPropertiesFromBrokerList(outputBrokerList),
              partitioner);
    }
    return new InfluxRowSink(new InfluxDBFactoryWrapper(), columnNames, configuration);
  }
}
