package com.gojek.daggers;

import com.gojek.daggers.sink.InfluxDBFactoryWrapper;
import com.gojek.daggers.sink.InfluxRowSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.types.Row;

public class SinkFactory {
  public static SinkFunction<Row> getSinkFunction(Configuration configuration, String[] columnNames) {
    String sink = configuration.getString("SINK_TYPE", "influx");
    if (sink.equals("kafka")) {
      String outputProtoPrefix = configuration.getString("OUTPUT_PROTO_CLASS_PREFIX", "");
      String outputBrokerList = configuration.getString("OUTPUT_KAFKA_BROKER", "");
      String outputTopic = configuration.getString("OUTPUT_KAFKA_TOPIC", "");
//      String outputBrokerList = "p-esb-kafka-mirror-b-01:6667";
//      String outputTopic = "test_demand_dagger_kafka_sink";
      ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoPrefix, columnNames);
      return new FlinkKafkaProducer010<>(outputBrokerList, outputTopic, protoSerializer);
    }
    return new InfluxRowSink(new InfluxDBFactoryWrapper(), columnNames, configuration);
  }
}
