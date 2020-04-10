package com.gojek.daggers.sink;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.metrics.telemetry.TelemetryTypes;
import com.gojek.daggers.sink.influx.InfluxDBFactoryWrapper;
import com.gojek.daggers.sink.influx.InfluxErrorHandler;
import com.gojek.daggers.sink.influx.InfluxRowSink;
import com.gojek.daggers.sink.log.LogSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

import java.util.*;

import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.*;
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

    public SinkFunction<Row> getSink(Configuration configuration, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator) {
        sinkType = configuration.getString("SINK_TYPE", "influx");
        addMetric(SINK_TYPE.getValue(), sinkType);

        switch (sinkType) {
            case "kafka":
                outputTopic = configuration.getString(OUTPUT_KAFKA_TOPIC, "");
                String outputProtoMessage = configuration.getString(OUTPUT_PROTO_MESSAGE, "");
                String outputStream = configuration.getString(OUTPUT_STREAM, "");
                addMetric(OUTPUT_TOPIC.getValue(), outputTopic);
                addMetric(OUTPUT_PROTO.getValue(), outputProtoMessage);
                addMetric(TelemetryTypes.OUTPUT_STREAM.getValue(), outputStream);

                ProtoSerializer protoSerializer = getProtoSerializer(configuration, columnNames, stencilClientOrchestrator);
                FlinkKafkaPartitioner partitioner = null;

                FlinkKafkaProducer010Custom<Row> flinkKafkaProducer = new FlinkKafkaProducer010Custom<Row>(outputTopic,
                        protoSerializer,
                        getProducerProperties(configuration),
                        partitioner,
                        configuration);
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
    private ProtoSerializer getProtoSerializer(Configuration configuration, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator) {
        // [PORTAL_MIGRATION] Move content inside this block to getSinkFunction method
        if (configuration.getString(PORTAL_VERSION, "1").equals("2")) {
            String outputProtoKey = configuration.getString(OUTPUT_PROTO_KEY, null);
            String outputProtoMessage = configuration.getString(OUTPUT_PROTO_MESSAGE, null);
            return new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator);
        }

        String outputProtoPrefix = configuration.getString(OUTPUT_PROTO_CLASS_PREFIX_KEY, "");
        return new ProtoSerializer(outputProtoPrefix, columnNames, stencilClientOrchestrator);
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    protected Properties getProducerProperties(Configuration configuration) {
        String outputBrokerList = configuration.getString(OUTPUT_KAFKA_BROKER, "");
        Properties kafkaProducerConfigs = FlinkKafkaProducerBase.getPropertiesFromBrokerList(outputBrokerList);
        if (configuration.getBoolean(PRODUCE_LARGE_MESSAGE_KEY, PRODUCE_LARGE_MESSAGE_DEFAULT)) {
            kafkaProducerConfigs.setProperty("compression.type", "snappy");
            kafkaProducerConfigs.setProperty("max.request.size", "5242880");
        }
        return kafkaProducerConfigs;
    }
}
