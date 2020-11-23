package com.gojek.daggers.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.metrics.telemetry.TelemetryTypes;
import com.gojek.daggers.sink.influx.ErrorHandler;
import com.gojek.daggers.sink.influx.InfluxDBFactoryWrapper;
import com.gojek.daggers.sink.influx.InfluxRowSink;
import com.gojek.daggers.sink.log.LogSink;

import java.util.*;

import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.*;
import static com.gojek.daggers.utils.Constants.OUTPUT_STREAM;
import static com.gojek.daggers.utils.Constants.*;

public class SinkOrchestrator implements TelemetryPublisher {

    private Map<String, List<String>> metrics = new HashMap<>();

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    public SinkFunction<Row> getSink(Configuration configuration, String[] columnNames, StencilClientOrchestrator stencilClientOrchestrator) {
        String sinkType = configuration.getString("SINK_TYPE", "influx");
        addMetric(SINK_TYPE.getValue(), sinkType);
        SinkFunction<Row> sink;
        switch (sinkType) {
            case "kafka":
                String outputTopic = configuration.getString(OUTPUT_KAFKA_TOPIC, "");
                String outputProtoKey = configuration.getString(OUTPUT_PROTO_KEY, null);
                String outputProtoMessage = configuration.getString(OUTPUT_PROTO_MESSAGE, "");
                String outputStream = configuration.getString(OUTPUT_STREAM, "");
                addMetric(OUTPUT_TOPIC.getValue(), outputTopic);
                addMetric(OUTPUT_PROTO.getValue(), outputProtoMessage);
                addMetric(TelemetryTypes.OUTPUT_STREAM.getValue(), outputStream);

                ProtoSerializer protoSerializer = new ProtoSerializer(outputProtoKey, outputProtoMessage, columnNames, stencilClientOrchestrator, outputTopic);
                FlinkKafkaProducer<Row> rowFlinkKafkaProducer = new FlinkKafkaProducer<>(outputTopic, protoSerializer, getProducerProperties(configuration), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
                sink = new FlinkKafkaProducerCustom(rowFlinkKafkaProducer, configuration);
                break;
            case "log":
                sink = new LogSink(columnNames);
                break;
            default:
                sink = new InfluxRowSink(new InfluxDBFactoryWrapper(), columnNames, configuration, new ErrorHandler());
        }
        notifySubscriber();
        return sink;
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    protected Properties getProducerProperties(Configuration configuration) {
        String outputBrokerList = configuration.getString(OUTPUT_KAFKA_BROKER, "");
        Properties kafkaProducerConfigs = FlinkKafkaProducerBase.getPropertiesFromBrokerList(outputBrokerList);
        if (configuration.getBoolean(PRODUCE_LARGE_MESSAGE_KEY, PRODUCE_LARGE_MESSAGE_DEFAULT)) {
            kafkaProducerConfigs.setProperty("compression.type", "snappy");
            kafkaProducerConfigs.setProperty("max.request.size", "20971520");
        }
        return kafkaProducerConfigs;
    }
}
