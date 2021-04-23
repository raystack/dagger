package io.odpf.dagger.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.types.Row;

import io.odpf.dagger.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.StencilClientOrchestrator;
import io.odpf.dagger.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.sink.influx.ErrorHandler;
import io.odpf.dagger.sink.influx.InfluxDBFactoryWrapper;
import io.odpf.dagger.sink.influx.InfluxRowSink;
import io.odpf.dagger.sink.log.LogSink;
import io.odpf.dagger.utils.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.odpf.dagger.metrics.telemetry.TelemetryTypes.OUTPUT_PROTO;
import static io.odpf.dagger.metrics.telemetry.TelemetryTypes.OUTPUT_TOPIC;
import static io.odpf.dagger.metrics.telemetry.TelemetryTypes.SINK_TYPE;

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
                String outputTopic = configuration.getString(Constants.OUTPUT_KAFKA_TOPIC, "");
                String outputProtoKey = configuration.getString(Constants.OUTPUT_PROTO_KEY, null);
                String outputProtoMessage = configuration.getString(Constants.OUTPUT_PROTO_MESSAGE, "");
                String outputStream = configuration.getString(Constants.OUTPUT_STREAM, "");
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
        String outputBrokerList = configuration.getString(Constants.OUTPUT_KAFKA_BROKER, "");
        Properties kafkaProducerConfigs = FlinkKafkaProducerBase.getPropertiesFromBrokerList(outputBrokerList);
        if (configuration.getBoolean(Constants.PRODUCE_LARGE_MESSAGE_KEY, Constants.PRODUCE_LARGE_MESSAGE_DEFAULT)) {
            kafkaProducerConfigs.setProperty("compression.type", "snappy");
            kafkaProducerConfigs.setProperty("max.request.size", "20971520");
        }
        return kafkaProducerConfigs;
    }
}
