package io.odpf.dagger.core.source.kafka.builder;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.source.DaggerSource;
import io.odpf.dagger.core.source.DaggerSourceFactory;
import io.odpf.dagger.core.source.Stream;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

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

    private DaggerSource createSource() {
        return DaggerSourceFactory.createDaggerSource(streamConfig, getDeserializationSchema(), configuration);
    }

    abstract KafkaDeserializationSchema getDeserializationSchema();

    DataTypes getInputDataType() {
        return DataTypes.valueOf(streamConfig.getDataType());
    }

    void addMetric(Map<String, List<String>> metrics, String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    protected Map<String, List<String>> addDefaultMetrics(Map<String, List<String>> metrics) {
        String topicPattern = streamConfig.getKafkaTopicNames();
        List<String> topicsToReport = new ArrayList<>();
        topicsToReport.addAll(Arrays.asList(topicPattern.split("\\|")));
        topicsToReport.forEach(topic -> addMetric(metrics, INPUT_TOPIC.getValue(), topic));

        String streamName = streamConfig.getKafkaName();
        addMetric(metrics, INPUT_STREAM.getValue(), streamName);
        return metrics;
    }
}
