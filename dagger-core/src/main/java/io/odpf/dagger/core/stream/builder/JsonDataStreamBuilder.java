package io.odpf.dagger.core.stream.builder;

import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.deserialization.json.JsonDeserializer;
import io.odpf.dagger.core.stream.StreamConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonDataStreamBuilder extends StreamBuilder {
    private final StreamConfig streamConfig;
    private final String streamName;

    private Map<String, List<String>> metrics = new HashMap<>();

    public JsonDataStreamBuilder(StreamConfig streamConfig, Configuration configuration) {
        super(streamConfig, configuration);
        this.streamConfig = streamConfig;

        // TODO : validate this streamName thing here
        streamName = streamConfig.getSchemaTable();
    }

    @Override
    void addTelemetry() {
        addDefaultMetrics(metrics);
    }

    @Override
    Map<String, List<String>> getMetrics() {
        return metrics;
    }

    @Override
    public boolean canBuild() {
        return getInputDataType() == DataTypes.JSON;
    }

    @Override
    DataTypes getInputDataType() {
        return DataTypes.valueOf(streamConfig.getStreamDataType());
    }

    @Override
    KafkaRecordDeserializationSchema getDeserializationSchema() {
        // TODO : update logic for json schema validation and all
        JsonDeserializer jsonDeserializer = new JsonDeserializer(streamConfig.getJsonSchema(), streamConfig.getRowTimeFieldName());

        return KafkaRecordDeserializationSchema.of(jsonDeserializer);
    }
}
