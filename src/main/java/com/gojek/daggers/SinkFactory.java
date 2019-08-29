package com.gojek.daggers;

import com.gojek.daggers.sink.influx.InfluxDBFactoryWrapper;
import com.gojek.daggers.sink.influx.InfluxErrorHandler;
import com.gojek.daggers.sink.influx.InfluxRowSink;
import com.gojek.daggers.sink.log.LogSink;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

import static com.gojek.daggers.Constants.OUTPUT_PROTO_CLASS_PREFIX_KEY;
import static com.gojek.daggers.Constants.OUTPUT_KAFKA_BROKER;
import static com.gojek.daggers.Constants.OUTPUT_KAFKA_TOPIC;
import static com.gojek.daggers.Constants.PORTAL_VERSION;
import static com.gojek.daggers.Constants.OUTPUT_PROTO_MESSAGE;
import static com.gojek.daggers.Constants.OUTPUT_PROTO_KEY;

public class SinkFactory {
    public static SinkFunction<Row> getSinkFunction(Configuration configuration, String[] columnNames, StencilClient stencilClient) {
        String sink = configuration.getString("SINK_TYPE", "influx");
        switch (sink) {
            case "kafka":
                String outputBrokerList = configuration.getString(OUTPUT_KAFKA_BROKER, "");
                String outputTopic = configuration.getString(OUTPUT_KAFKA_TOPIC, "");

                ProtoSerializer protoSerializer = SinkFactory.protoSerializerFactory(configuration, columnNames, stencilClient);

                // Use kafka partitioner
                FlinkKafkaPartitioner partitioner = null;

                FlinkKafkaProducer010<Row> flinkKafkaProducer = new FlinkKafkaProducer010<Row>(outputTopic,
                        protoSerializer,
                        FlinkKafkaProducerBase.getPropertiesFromBrokerList(outputBrokerList),
                        partitioner);

                // don't commit to source kafka if we can publish to output kafka
                flinkKafkaProducer.setFlushOnCheckpoint(true);

                return flinkKafkaProducer;
            case "log":
                return new LogSink(columnNames);
            default:
                return new InfluxRowSink(new InfluxDBFactoryWrapper(), columnNames, configuration, new InfluxErrorHandler());
        }
    }
    // TODO: [PORTAL_MIGRATION] Remove this switch when migration to new portal is done
    private static ProtoSerializer protoSerializerFactory(Configuration configuration, String[] columnNames, StencilClient stencilClient) {
        // [PORTAL_MIGRATION] Move content inside this block to getSinkFunction method
        if (configuration.getString(PORTAL_VERSION, "1").equals("2")) {
            String outputProtoKey = configuration.getString(OUTPUT_PROTO_KEY, null);
            String outputProtoMessage = configuration.getString(OUTPUT_PROTO_MESSAGE, null);
            return new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClient);
        }

        String outputProtoPrefix = configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "");
        return new ProtoSerializer(outputProtoPrefix, columnNames, stencilClient);
    }
}
