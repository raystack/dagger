package io.odpf.dagger.core.stream.builder;

import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.common.serde.deserialization.json.JsonDeserializer;
import io.odpf.dagger.core.stream.StreamMetaData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_TABLE;
import static io.odpf.dagger.core.utils.Constants.STREAM_INPUT_DATATYPE;
import static io.odpf.dagger.core.utils.Constants.STREAM_JSON_SCHEMA_KEY;
import static io.odpf.dagger.core.utils.Constants.STREAM_ROW_TIME_FIELD_NAME_KEY;

public class JsonDataStreamBuilder extends StreamBuilder {
    private final Map<String, String> streamConfig;
    private final String streamName;
    private StreamMetaData streamMetaData;
    private Map<String, List<String>> metrics = new HashMap<>();

    public JsonDataStreamBuilder(Map<String, String> streamConfig, StreamMetaData streamMetaData) {
        super(streamMetaData, streamConfig);
        this.streamConfig = streamConfig;
        this.streamMetaData = streamMetaData;
        streamName = streamConfig.getOrDefault(STREAM_INPUT_SCHEMA_TABLE, "");
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
        return DataTypes.valueOf(streamConfig.getOrDefault(STREAM_INPUT_DATATYPE, "PROTO"));
    }

    @Override
    KafkaRecordDeserializationSchema getDeserializationSchema() {
        // TODO : update logic for json schema validation and all

        String jsonSchema = streamConfig.getOrDefault(STREAM_JSON_SCHEMA_KEY, "");
        String jsonRowTimeFieldName = streamConfig.getOrDefault(STREAM_ROW_TIME_FIELD_NAME_KEY, "");
        JsonDeserializer jsonDeserializer = new JsonDeserializer(jsonSchema, jsonRowTimeFieldName);

        return KafkaRecordDeserializationSchema.of(jsonDeserializer);
    }
}
