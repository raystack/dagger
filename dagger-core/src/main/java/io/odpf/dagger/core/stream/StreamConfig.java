package io.odpf.dagger.core.stream;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import io.odpf.dagger.common.configuration.Configuration;
import lombok.Getter;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.StringReader;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static io.odpf.dagger.core.utils.Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY;
import static io.odpf.dagger.core.utils.Constants.SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES_KEY;

public class StreamConfig {
    private static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setPrettyPrinting()
            .create();

    private static final String KAFKA_PREFIX = "source_kafka_consumer_config_";

    @SerializedName("SOURCE_KAFKA_TOPIC_NAMES")
    @Getter
    private String kafkaTopic;

    @SerializedName("INPUT_SCHEMA_PROTO_CLASS")
    @Getter
    private String protoClass;

    @SerializedName("INPUT_SCHEMA_TABLE")
    @Getter
    private String schemaTable;

    @SerializedName("SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE")
    @Getter
    private String autoCommitEnabled;

    @SerializedName("SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET")
    @Getter
    private String autoOffsetReset;

    @SerializedName("SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID")
    @Getter
    private String consumerGroup;

    @SerializedName("SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS")
    @Getter
    private String sourceBootstrapServers;

    @SerializedName("SOURCE_KAFKA_NAME")
    @Getter
    private String sourceKafkaName;

    @SerializedName("SOURCE_JSON_SCHEMA")
    @Getter
    private String jsonSchema;

    @SerializedName("SOURCE_ROW_TIME_FIELD")
    @Getter
    private String rowTimeFieldName;

    @SerializedName("INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX")
    @Getter
    private String eventTimestampFieldIndex;

    @SerializedName("SOURCE_DATATYPE")
    private String streamDataType;

    public String getStreamDataType() {
        if (streamDataType == null) {
            streamDataType = "PROTO";
        }
        return streamDataType;
    }

    public static StreamConfig[] parse(Configuration configuration) {
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        JsonReader reader = new JsonReader(new StringReader(jsonArrayString));
        reader.setLenient(true);

        return GSON.fromJson(jsonArrayString, StreamConfig[].class);
    }

    public Properties getKafkaProps(Configuration configuration) {
        String jsonString = GSON.toJson(this);
        Map<String, String> streamConfigMap = GSON.fromJson(jsonString, Map.class);
        Properties kafkaProps = new Properties();
        streamConfigMap.entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().startsWith(KAFKA_PREFIX))
                .forEach(e -> kafkaProps.setProperty(parseVarName(e.getKey(), KAFKA_PREFIX), e.getValue()));
        setAdditionalConfigs(kafkaProps, configuration);
        return kafkaProps;
    }

    private String parseVarName(String varName, String kafkaPrefix) {
        String[] names = varName.toLowerCase().replaceAll(kafkaPrefix, "").split("_");
        return String.join(".", names);
    }

    private void setAdditionalConfigs(Properties kafkaProps, Configuration configuration) {
        if (configuration.getBoolean(SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)) {
            kafkaProps.setProperty(SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES_KEY, SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES_DEFAULT);
        }
    }

    public Pattern getTopicPattern() {
        return Pattern.compile(kafkaTopic);
    }

    public OffsetsInitializer getStartingOffset() {
        return OffsetsInitializer.committedOffsets(getOffsetResetStrategy());
    }

    private OffsetResetStrategy getOffsetResetStrategy() {
        return OffsetResetStrategy.valueOf(autoOffsetReset.toUpperCase());
    }
}
