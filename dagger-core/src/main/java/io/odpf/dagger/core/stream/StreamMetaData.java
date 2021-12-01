package io.odpf.dagger.core.stream;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import io.odpf.dagger.common.configuration.Configuration;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static io.odpf.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_TABLE;
import static io.odpf.dagger.core.utils.Constants.*;

// TODO : Delete this class
public class StreamMetaData {
    private Map<String, String> streamConfig;
    private Configuration configuration;
    private static final String KAFKA_PREFIX = "source_kafka_consumer_config_";

    public StreamMetaData(Map<String, String> streamConfig, Configuration configuration) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
    }

    public String getTableName() {
        return streamConfig.getOrDefault(STREAM_INPUT_SCHEMA_TABLE, "");
    }

    public Properties getKafkaProps() {
        Properties kafkaProps = new Properties();
        streamConfig.entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().startsWith(KAFKA_PREFIX))
                .forEach(e -> kafkaProps.setProperty(parseVarName(e.getKey(), KAFKA_PREFIX), e.getValue()));
        setAdditionalConfigs(kafkaProps);
        return kafkaProps;
    }

    public Pattern getTopicPattern() {
        String topicsForStream = streamConfig.getOrDefault(STREAM_SOURCE_KAFKA_TOPIC_NAMES_KEY, "");
        return Pattern.compile(topicsForStream);
    }


    private void setAdditionalConfigs(Properties kafkaProps) {
        if (configuration.getBoolean(SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)) {
            kafkaProps.setProperty(SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES_KEY, SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES_DEFAULT);
        }
    }

    private String parseVarName(String varName, String kafkaPrefix) {
        String[] names = varName.toLowerCase().replaceAll(kafkaPrefix, "").split("_");
        return String.join(".", names);
    }

    public OffsetsInitializer getStartingOffset() {
        return OffsetsInitializer.committedOffsets(getOffsetResetStrategy());
    }

    private OffsetResetStrategy getOffsetResetStrategy() {
        String consumerOffsetResetStrategy = streamConfig.getOrDefault(SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET_KEY, SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET_DEFAULT);
        OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.valueOf(consumerOffsetResetStrategy.toUpperCase());
        return offsetResetStrategy;
    }
}
