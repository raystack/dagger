package com.gojek.daggers.sink;

import com.gojek.daggers.metrics.TelemetryPublisher;
import com.gojek.daggers.metrics.TelemetryTypes;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.metrics.TelemetryTypes.*;
import static com.gojek.daggers.utils.Constants.OUTPUT_STREAM;
import static com.gojek.daggers.utils.Constants.*;

public class SinkOrchestrator implements TelemetryPublisher {

    private Map<String, List<String>> metrics = new HashMap<>();
    private String sinkType;
    private String outputTopic;
    private SinkFunction<Row> sink;

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    public SinkFunction<Row> getSink(Configuration configuration, String[] columnNames, StencilClient stencilClient) {
        sinkType = configuration.getString("SINK_TYPE", "influx");
        addMetric(SINK_TYPE.getValue(), sinkType);

        switch (sinkType) {
            case "kafka":
                String outputBrokerList = configuration.getString(OUTPUT_KAFKA_BROKER, "");
                outputTopic = configuration.getString(OUTPUT_KAFKA_TOPIC, "");
                String outputProtoMessage = configuration.getString(OUTPUT_PROTO_MESSAGE, "");
                String outputStream = configuration.getString(OUTPUT_STREAM, "");
                addMetric(OUTPUT_TOPIC.getValue(), outputTopic);
                addMetric(OUTPUT_PROTO.getValue(), outputProtoMessage);
                addMetric(TelemetryTypes.OUTPUT_STREAM.getValue(), outputStream);

                ProtoSerializer protoSerializer = getProtoSerializer(configuration, columnNames, stencilClient);
                FlinkKafkaPartitioner partitioner = null;

                FlinkKafkaProducer010<Row> flinkKafkaProducer = new FlinkKafkaProducer010<Row>(outputTopic,
                        protoSerializer,
                        FlinkKafkaProducerBase.getPropertiesFromBrokerList(outputBrokerList),
                        partitioner);
                flinkKafkaProducer.setFlushOnCheckpoint(true);
                sink = flinkKafkaProducer;
                break;
            case "log":
                sink = new LogSink(columnNames);
                break;
            default:
                sink = new InfluxRowSink(new InfluxDBFactoryWrapper(), columnNames, configuration, new InfluxErrorHandler());
        }
        notifySubscriber();
        return sink;
    }

    // TODO: [PORTAL_MIGRATION] Remove this switch when migration to new portal is done
    private ProtoSerializer getProtoSerializer(Configuration configuration, String[] columnNames, StencilClient stencilClient) {
        // [PORTAL_MIGRATION] Move content inside this block to getSinkFunction method
        if (configuration.getString(PORTAL_VERSION, "1").equals("2")) {
            String outputProtoKey = configuration.getString(OUTPUT_PROTO_KEY, null);
            String outputProtoMessage = configuration.getString(OUTPUT_PROTO_MESSAGE, null);
            return new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClient);
        }

        String outputProtoPrefix = configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "");
        return new ProtoSerializer(outputProtoPrefix, columnNames, stencilClient);
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

}
