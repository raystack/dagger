package com.gotocompany.dagger.core.source.config;

import com.google.gson.annotations.JsonAdapter;
import com.gotocompany.dagger.core.source.config.adapter.DaggerSASLMechanismAdaptor;
import com.gotocompany.dagger.core.source.config.adapter.DaggerSSLKeyStoreFileTypeAdaptor;
import com.gotocompany.dagger.core.source.config.adapter.DaggerSSLProtocolAdaptor;
import com.gotocompany.dagger.core.source.config.adapter.DaggerSSLTrustStoreFileTypeAdaptor;
import com.gotocompany.dagger.core.source.config.adapter.DaggerSecurityProtocolAdaptor;
import com.gotocompany.dagger.core.source.config.adapter.FileDateRangeAdaptor;
import com.gotocompany.dagger.core.source.config.adapter.SourceParquetFilePathsAdapter;
import com.gotocompany.dagger.core.source.config.models.SourceType;
import com.gotocompany.dagger.core.source.parquet.SourceParquetReadOrderStrategy;
import com.gotocompany.dagger.core.source.parquet.SourceParquetSchemaMatchStrategy;
import com.gotocompany.dagger.core.source.config.models.SourceDetails;
import com.gotocompany.dagger.core.source.config.models.SourceName;
import com.gotocompany.dagger.core.source.config.models.TimeRangePool;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.gotocompany.dagger.common.configuration.Configuration;
import lombok.Getter;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.StringReader;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.gotocompany.dagger.common.core.Constants.INPUT_STREAMS;
import static com.gotocompany.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_PROTO_CLASS;
import static com.gotocompany.dagger.common.core.Constants.STREAM_INPUT_SCHEMA_TABLE;
import static com.gotocompany.dagger.core.utils.Constants.*;
import static com.gotocompany.dagger.core.utils.Constants.STREAM_SOURCE_PARQUET_FILE_DATE_RANGE_KEY;

public class StreamConfig {
    private static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setPrettyPrinting()
            .create();

    private static final String KAFKA_PREFIX = "source_kafka_consumer_config_";

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEY_PASSWORD_KEY)
    @Getter
    private String sslKeyPassword;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_LOCATION_KEY)
    @Getter
    private String sslKeystoreLocation;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_PASSWORD_KEY)
    @Getter
    private String sslKeystorePassword;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SSL_KEYSTORE_TYPE_KEY)
    @Getter
    @JsonAdapter(value = DaggerSSLKeyStoreFileTypeAdaptor.class)
    private String sslKeystoreType;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SSL_PROTOCOL_KEY)
    @Getter
    @JsonAdapter(value = DaggerSSLProtocolAdaptor.class)
    private String sslProtocol;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_LOCATION_KEY)
    @Getter
    private String sslTruststoreLocation;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_PASSWORD_KEY)
    @Getter
    private String sslTruststorePassword;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SSL_TRUSTSTORE_TYPE_KEY)
    @Getter
    @JsonAdapter(value = DaggerSSLTrustStoreFileTypeAdaptor.class)
    private String sslTruststoreType;

    @SerializedName(STREAM_SOURCE_KAFKA_TOPIC_NAMES_KEY)
    @Getter
    private String kafkaTopicNames;

    @SerializedName(STREAM_INPUT_SCHEMA_PROTO_CLASS)
    @Getter
    private String protoClass;

    @SerializedName(STREAM_INPUT_SCHEMA_TABLE)
    @Getter
    private String schemaTable;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE_KEY)
    @Getter
    private String autoCommitEnable;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET_KEY)
    private String autoOffsetReset;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID_KEY)
    @Getter
    private String consumerGroupId;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS_KEY)
    @Getter
    private String bootstrapServers;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SECURITY_PROTOCOL_KEY)
    @Getter
    @JsonAdapter(value = DaggerSecurityProtocolAdaptor.class)
    private String securityProtocol;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SASL_MECHANISM_KEY)
    @JsonAdapter(value = DaggerSASLMechanismAdaptor.class)
    @Getter
    private String saslMechanism;

    @SerializedName(SOURCE_KAFKA_CONSUMER_CONFIG_SASL_JAAS_CONFIG_KEY)
    @Getter
    private String saslJaasConfig;

    @SerializedName(STREAM_INPUT_STREAM_NAME_KEY)
    @Getter
    private String kafkaName;

    @SerializedName(STREAM_INPUT_SCHEMA_JSON_SCHEMA_KEY)
    @Getter
    private String jsonSchema;

    @SerializedName(STREAM_INPUT_SCHEMA_JSON_EVENT_TIMESTAMP_FIELD_NAME_KEY)
    @Getter
    private String jsonEventTimestampFieldName;

    @SerializedName(STREAM_INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX_KEY)
    @Getter
    private String eventTimestampFieldIndex;

    @SerializedName(STREAM_INPUT_DATATYPE)
    private String dataType;

    @SerializedName(STREAM_SOURCE_DETAILS_KEY)
    private SourceDetails[] sourceDetails;

    @SerializedName(STREAM_SOURCE_PARQUET_FILE_PATHS_KEY)
    @Getter
    @JsonAdapter(value = SourceParquetFilePathsAdapter.class)
    private String[] parquetFilePaths;

    @SerializedName(STREAM_SOURCE_PARQUET_READ_ORDER_STRATEGY_KEY)
    private SourceParquetReadOrderStrategy parquetFilesReadOrderStrategy;

    @SerializedName(STREAM_SOURCE_PARQUET_SCHEMA_MATCH_STRATEGY_KEY)
    @Getter
    private SourceParquetSchemaMatchStrategy parquetSchemaMatchStrategy;

    @SerializedName(STREAM_SOURCE_PARQUET_FILE_DATE_RANGE_KEY)
    @JsonAdapter(FileDateRangeAdaptor.class)
    @Getter
    private TimeRangePool parquetFileDateRange;

    public String getDataType() {
        if (dataType == null) {
            dataType = "PROTO";
        }
        return dataType;
    }

    public SourceDetails[] getSourceDetails() {
        if (sourceDetails == null) {
            return new SourceDetails[]{new SourceDetails(SourceName.KAFKA_CONSUMER, SourceType.UNBOUNDED)};
        } else {
            return sourceDetails;
        }
    }

    public SourceParquetReadOrderStrategy getParquetFilesReadOrderStrategy() {
        if (parquetFilesReadOrderStrategy == null) {
            return SourceParquetReadOrderStrategy.EARLIEST_TIME_URL_FIRST;
        } else {
            return parquetFilesReadOrderStrategy;
        }
    }

    public String getAutoOffsetReset() {
        if (autoOffsetReset == null) {
            autoOffsetReset = "latest";
        }
        return autoOffsetReset;
    }

    public static StreamConfig[] parse(Configuration configuration) {
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        JsonReader reader = new JsonReader(new StringReader(jsonArrayString));
        reader.setLenient(true);

        return Stream.of(GSON.fromJson(jsonArrayString, StreamConfig[].class))
                .map(StreamConfigValidator::validateSourceDetails)
                .map(StreamConfigValidator::validateParquetDataSourceStreamConfigs)
                .toArray(StreamConfig[]::new);
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
        return Pattern.compile(kafkaTopicNames);
    }

    public OffsetsInitializer getStartingOffset() {
        return OffsetsInitializer.committedOffsets(getOffsetResetStrategy());
    }

    private OffsetResetStrategy getOffsetResetStrategy() {
        return OffsetResetStrategy.valueOf(autoOffsetReset.toUpperCase());
    }
}
