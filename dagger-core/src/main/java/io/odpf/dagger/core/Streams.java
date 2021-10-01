package io.odpf.dagger.core;

import com.google.gson.Gson;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.source.FlinkKafkaConsumerCustom;
import io.odpf.dagger.core.source.ProtoDeserializer;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static io.odpf.dagger.common.core.Constants.*;
import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.*;
import static io.odpf.dagger.core.utils.Constants.*;

/**
 * The Streams.
 */
public class Streams implements TelemetryPublisher {
    private static final String KAFKA_PREFIX = "source_kafka_consumer_config_";
    private final Configuration configuration;
    private Map<String, FlinkKafkaConsumerCustom> streams = new HashMap<>();
    private LinkedHashMap<String, String> protoClassForTable = new LinkedHashMap<>();
    private StencilClientOrchestrator stencilClientOrchestrator;
    private boolean enablePerPartitionWatermark;
    private long watermarkDelay;
    private Map<String, List<String>> metrics = new HashMap<>();
    private List<String> topics = new ArrayList<>();
    private List<String> protoClassNames = new ArrayList<>();
    private List<String> streamNames = new ArrayList<>();
    private static final Gson GSON = new Gson();

    /**
     * Instantiates a new Streams.
     *
     * @param configuration               the configuration
     * @param rowTimeAttributeName        the row time attribute name
     * @param stencilClientOrchestrator   the stencil client orchestrator
     * @param enablePerPartitionWatermark the enable per partition watermark
     * @param watermarkDelay              the watermark delay
     */
    public Streams(Configuration configuration, String rowTimeAttributeName, StencilClientOrchestrator stencilClientOrchestrator, boolean enablePerPartitionWatermark, long watermarkDelay) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.watermarkDelay = watermarkDelay;
        this.enablePerPartitionWatermark = enablePerPartitionWatermark;
        this.configuration = configuration;
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Map[] streamsConfig = GSON.fromJson(jsonArrayString, Map[].class);
        for (Map<String, String> streamConfig : streamsConfig) {
            String tableName = streamConfig.getOrDefault(STREAM_INPUT_SCHEMA_TABLE, "");
            streams.put(tableName, getKafkaConsumer(rowTimeAttributeName, streamConfig));
        }
    }

    /**
     * Gets streams.
     *
     * @return the streams
     */
    public Map<String, FlinkKafkaConsumerCustom> getStreams() {
        return streams;
    }

    /**
     * Gets protos.
     *
     * @return the protos
     */
    public LinkedHashMap<String, String> getProtos() {
        return protoClassForTable;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addTelemetry();
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    private static String parseVarName(String varName, String kafkaPrefix) {
        String[] names = varName.toLowerCase().replaceAll(kafkaPrefix, "").split("_");
        return String.join(".", names);
    }

    // TODO : refactor the watermark related things
    private FlinkKafkaConsumerCustom getKafkaConsumer(String rowTimeAttributeName, Map<String, String> streamConfig) {
        String topicsForStream = streamConfig.getOrDefault(STREAM_SOURCE_KAFKA_TOPIC_NAMES_KEY, "");
        topics.add(topicsForStream);
        String protoClassName = streamConfig.getOrDefault(STREAM_INPUT_SCHEMA_PROTO_CLASS, "");
        protoClassNames.add(protoClassName);
        streamNames.add(streamConfig.getOrDefault(INPUT_STREAM_NAME_KEY, ""));
        String tableName = streamConfig.getOrDefault(STREAM_INPUT_SCHEMA_TABLE, "");
        protoClassForTable.put(tableName, protoClassName);
        int timestampFieldIndex = Integer.parseInt(streamConfig.getOrDefault(STREAM_INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX_KEY, ""));
        Properties kafkaProps = new Properties();
        streamConfig.entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().startsWith(KAFKA_PREFIX))
                .forEach(e -> kafkaProps.setProperty(parseVarName(e.getKey(), KAFKA_PREFIX), e.getValue()));

        setAdditionalConfigs(kafkaProps);

        FlinkKafkaConsumerCustom fc = new FlinkKafkaConsumerCustom(Pattern.compile(topicsForStream),
                new ProtoDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClientOrchestrator), kafkaProps, configuration);

        // https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html#timestamps-per-kafka-partition
        if (enablePerPartitionWatermark) {
            Duration maxOutOfOrderliness = Duration.ofMillis(watermarkDelay);
            DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class);

            fc.assignTimestampsAndWatermarks(WatermarkStrategy.
                    <Row>forBoundedOutOfOrderness(maxOutOfOrderliness)
                    .withTimestampAssigner((SerializableTimestampAssigner<Row>) (element, recordTimestamp) -> {
                        int index = element.getArity() - 1;
                        return ((Timestamp) element.getField(index)).getTime();
                    }));
        }

        return fc;
    }

    private void setAdditionalConfigs(Properties kafkaProps) {
        if (configuration.getBoolean(SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_KEY, SOURCE_KAFKA_CONSUME_LARGE_MESSAGE_ENABLE_DEFAULT)) {
            kafkaProps.setProperty(SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES_KEY, SOURCE_KAFKA_MAX_PARTITION_FETCH_BYTES_DEFAULT);
        }
    }

    private void addTelemetry() {
        List<String> topicsToReport = new ArrayList<>();
        topics.forEach(topicsPerStream -> topicsToReport.addAll(Arrays.asList(topicsPerStream.split("\\|"))));
        topicsToReport.forEach(topic -> addMetric(INPUT_TOPIC.getValue(), topic));
        protoClassNames.forEach(protoClassName -> addMetric(INPUT_PROTO.getValue(), protoClassName));
        streamNames.forEach(streamName -> addMetric(INPUT_STREAM.getValue(), streamName));
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
