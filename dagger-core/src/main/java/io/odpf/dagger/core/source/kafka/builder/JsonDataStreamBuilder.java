package io.odpf.dagger.core.source.kafka.builder;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.json.deserialization.JsonDeserializer;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonDataStreamBuilder extends StreamBuilder {
    private final StreamConfig streamConfig;

    private Map<String, List<String>> metrics = new HashMap<>();

    public JsonDataStreamBuilder(StreamConfig streamConfig, Configuration configuration) {
        super(streamConfig, configuration);
        this.streamConfig = streamConfig;
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
    KafkaDeserializationSchema<Row> getDeserializationSchema() {
        return new JsonDeserializer(streamConfig.getJsonSchema(), streamConfig.getJsonEventTimestampFieldName());
    }
}
