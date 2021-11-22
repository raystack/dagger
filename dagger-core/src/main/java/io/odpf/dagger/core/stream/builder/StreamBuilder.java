package io.odpf.dagger.core.stream.builder;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.stream.Stream;
import io.odpf.dagger.core.stream.StreamMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_STREAM;
import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_TOPIC;
import static io.odpf.dagger.core.utils.Constants.STREAM_INPUT_DATATYPE;

public abstract class StreamBuilder implements TelemetryPublisher {

    private StreamMetaData streamMetaData;
    private Map<String, String> streamConfig;

    public StreamBuilder(StreamMetaData streamMetaData, Map<String, String> streamConfig) {
        this.streamMetaData = streamMetaData;
        this.streamConfig = streamConfig;
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
        return new Stream(createSource(), streamMetaData.getTableName(), getInputDataType());
    }

    private KafkaSource createSource() {
        return KafkaSource.<Row>builder()
                .setTopicPattern(streamMetaData.getTopicPattern())
                .setStartingOffsets(streamMetaData.getStartingOffset())
                .setProperties(streamMetaData.getKafkaProps())
                .setDeserializer(getDeserializationSchema())
                .build();
    }

    abstract KafkaRecordDeserializationSchema getDeserializationSchema();

    DataTypes getInputDataType() {
        return DataTypes.valueOf(streamConfig.getOrDefault(STREAM_INPUT_DATATYPE, "PROTO"));
    }

    void addMetric(Map<String, List<String>> metrics, String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    protected Map<String, List<String>> addDefaultMetrics(Map<String, List<String>> metrics) {
        String topicPattern = streamMetaData.getTopicPattern().pattern();
        List<String> topicsToReport = new ArrayList<>();
        topicsToReport.addAll(Arrays.asList(topicPattern.split("\\|")));
        topicsToReport.forEach(topic -> addMetric(metrics, INPUT_TOPIC.getValue(), topic));

        // TODO : validate streamName
        String streamName = streamMetaData.getTableName();
        addMetric(metrics, INPUT_STREAM.getValue(), streamName);
        return metrics;
    }
}
