package io.odpf.dagger.core.source.builder;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.source.*;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_STREAM;
import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_TOPIC;

public abstract class StreamBuilder implements TelemetryPublisher {

    private StreamConfig streamConfig;
    private Configuration configuration;
    private StencilClientOrchestrator stencilClientOrchestrator;

    public StreamBuilder(StreamConfig streamConfig, Configuration configuration) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.stencilClientOrchestrator = null;
    }

    public StreamBuilder(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
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

    public Stream buildStream() {
        return new Stream(createDataSource(), streamConfig.getSourceDetails(), streamConfig.getSchemaTable(), getInputDataType());
    }

    private Source createSource() {
        return KafkaSource.<Row>builder()
                .setTopicPattern(streamConfig.getTopicPattern())
                .setStartingOffsets(streamConfig.getStartingOffset())
                .setProperties(streamConfig.getKafkaProps(configuration))
                .setDeserializer(getDeserializationSchema())
                .build();
    }

    private Source createDataSource() {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        ArrayList<Source> sourceList = new ArrayList<>();
        for (SourceDetails sourceDetails : sourceDetailsArray) {
            SourceName sourceName = sourceDetails.getSourceName();
            DataTypes inputDataType = getInputDataType();
            DaggerDeserializer<Row> deserializer = DeserializerFactory.create(sourceName, inputDataType, streamConfig, configuration, stencilClientOrchestrator);
            Source source = SourceFactory.create(sourceDetails, streamConfig, configuration, deserializer);
            sourceList.add(source);
        }
        if(sourceList.size() > 1) {
            throw new IllegalConfigurationException("Invalid stream configuration: Multiple back to back data sources is not supported yet.");
        }
        return sourceList.get(0);
    }

    abstract KafkaRecordDeserializationSchema getDeserializationSchema();

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
