package io.odpf.dagger.core;

import com.google.gson.Gson;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.source.FlinkKafkaConsumerCustom;
import io.odpf.dagger.core.source.ProtoDeserializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
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

public class Streams implements TelemetryPublisher {
    private static final String KAFKA_PREFIX = "kafka_consumer_config_";
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

    public Streams(Configuration configuration, String rowTimeAttributeName, StencilClientOrchestrator stencilClientOrchestrator, boolean enablePerPartitionWatermark, long watermarkDelay) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.watermarkDelay = watermarkDelay;
        this.enablePerPartitionWatermark = enablePerPartitionWatermark;
        this.configuration = configuration;
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        Map[] streamsConfig = GSON.fromJson(jsonArrayString, Map[].class);
        for (Map<String, String> streamConfig : streamsConfig) {
            String tableName = streamConfig.getOrDefault(STREAM_TABLE_NAME, "");
            streams.put(tableName, getKafkaConsumer(rowTimeAttributeName, streamConfig));
        }
    }

    public Map<String, FlinkKafkaConsumerCustom> getStreams() {
        return streams;
    }

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

    private FlinkKafkaConsumerCustom getKafkaConsumer(String rowTimeAttributeName, Map<String, String> streamConfig) {
        String topicsForStream = streamConfig.getOrDefault(STREAM_TOPIC_NAMES, "");
        topics.add(topicsForStream);
        String protoClassName = streamConfig.getOrDefault(STREAM_PROTO_CLASS_NAME, "");
        protoClassNames.add(protoClassName);
        streamNames.add(streamConfig.getOrDefault(INPUT_STREAM_NAME, ""));
        String tableName = streamConfig.getOrDefault(STREAM_TABLE_NAME, "");
        protoClassForTable.put(tableName, protoClassName);
        int timestampFieldIndex = Integer.parseInt(streamConfig.getOrDefault("EVENT_TIMESTAMP_FIELD_INDEX", ""));
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
            fc.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.of(watermarkDelay, TimeUnit.MILLISECONDS)) {
                @Override
                public long extractTimestamp(Row element) {
                    int index = element.getArity() - 1;
                    return ((Timestamp) element.getField(index)).getTime();
                }
            });
        }

        return fc;
    }

    private void setAdditionalConfigs(Properties kafkaProps) {
        if (configuration.getBoolean(CONSUME_LARGE_MESSAGE_KEY, CONSUME_LARGE_MESSAGE_DEFAULT)) {
            kafkaProps.setProperty("max.partition.fetch.bytes", "5242880");
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
