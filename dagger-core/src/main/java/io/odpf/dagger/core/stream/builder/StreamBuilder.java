package io.odpf.dagger.core.stream.builder;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.stream.Stream;
import io.odpf.dagger.core.stream.StreamConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_STREAM;
import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_TOPIC;

public abstract class StreamBuilder implements TelemetryPublisher {

    private StreamConfig streamConfig;
    private Configuration configuration;

    public StreamBuilder(StreamConfig streamConfig, Configuration configuration) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addTelemetry();
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return getMetrics();
    }

    abstract void addTelemetry();

    abstract Map<String, List<String>> getMetrics();

    public abstract boolean canBuild();

    public Stream build() {
        return new Stream(createSource(), streamConfig.getSchemaTable(), getInputDataType());
    }

    private KafkaSource createSource() {
        return KafkaSource.<Row>builder()
                .setTopicPattern(streamConfig.getTopicPattern())
                .setStartingOffsets(streamConfig.getStartingOffset())
                .setProperties(streamConfig.getKafkaProps(configuration))
                .setDeserializer(getDeserializationSchema())
                .build();
    }

    abstract KafkaRecordDeserializationSchema getDeserializationSchema();

    DataTypes getInputDataType() {
        return DataTypes.valueOf(streamConfig.getStreamDataType());
    }

    void addMetric(Map<String, List<String>> metrics, String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    protected Map<String, List<String>> addDefaultMetrics(Map<String, List<String>> metrics) {
        String topicPattern = streamConfig.getKafkaTopic();
        List<String> topicsToReport = new ArrayList<>();
        topicsToReport.addAll(Arrays.asList(topicPattern.split("\\|")));
        topicsToReport.forEach(topic -> addMetric(metrics, INPUT_TOPIC.getValue(), topic));

        String streamName = streamConfig.getSourceKafkaName();
        addMetric(metrics, INPUT_STREAM.getValue(), streamName);
        return metrics;
    }
}
