package com.gojek.daggers;


import com.gojek.de.stencil.StencilClient;
import com.google.gson.Gson;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class Streams {

    public static final int DEFAULT_WATERMARK_DELAY = 10000;
    private Map<String, FlinkKafkaConsumer011<Row>> streams = new HashMap<>();
    private LinkedHashMap<String, String> protoClassForTable = new LinkedHashMap<>();
    private static final String KAFKA_PREFIX = "kafka_consumer_config_";
    private Configuration configuration;
    private StencilClient stencilClient;

    public Streams(Configuration configuration, String rowTimeAttributeName, StencilClient stencilClient) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
        String jsonArrayString = configuration.getString("STREAMS", "");
        Gson gson = new Gson();
        Map[] streamsConfig = gson.fromJson(jsonArrayString, Map[].class);
        for (Map<String, String> streamConfig : streamsConfig) {
            String tableName = streamConfig.getOrDefault("TABLE_NAME", "");
            streams.put(tableName, getKafkaConsumer(rowTimeAttributeName, streamConfig));
        }
    }

    private FlinkKafkaConsumer011<Row> getKafkaConsumer(String rowTimeAttributeName, Map<String, String> streamConfig) {
        String topics = streamConfig.getOrDefault("TOPIC_NAMES", "");
        String tableName = streamConfig.getOrDefault("TABLE_NAME", "");
        String protoClassName = streamConfig.getOrDefault("PROTO_CLASS_NAME", "");
        protoClassForTable.put(tableName, protoClassName);
        int timestampFieldIndex = Integer.parseInt(streamConfig.getOrDefault("EVENT_TIMESTAMP_FIELD_INDEX", ""));
        Properties kafkaProps = new Properties();
        streamConfig.entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().startsWith(KAFKA_PREFIX))
                .forEach(e -> kafkaProps.setProperty(parseVarName(e.getKey(), KAFKA_PREFIX), e.getValue()));

        FlinkKafkaConsumer011 fc = new FlinkKafkaConsumer011<>(Pattern.compile(topics), new ProtoDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName, stencilClient), kafkaProps);

        // https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html#timestamps-per-kafka-partition
        if (!configuration.getBoolean("DISABLE_PARTITION_WATERMARK", false)) {
            long watermarkDelay = configuration.getLong("WATERMARK_DELAY_MS", DEFAULT_WATERMARK_DELAY);
            fc.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
                @Override
                public long extractAscendingTimestamp(Row element) {
                    int index = element.getArity() - 1;
                    long ts = ((Timestamp) element.getField(index)).getTime();
                    return ts - watermarkDelay;
                }
            });
        }

        return fc;
    }

    public Map<String, FlinkKafkaConsumer011<Row>> getStreams() {
        return streams;
    }

    public LinkedHashMap<String, String> getProtos() {
        return protoClassForTable;
    }

    private static String parseVarName(String varName, String kafkaPrefix) {
        String[] names = varName.toLowerCase().replaceAll(kafkaPrefix, "").split("_");
        return String.join(".", names);
    }
}
