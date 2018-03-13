package com.gojek.daggers;


import com.google.gson.Gson;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class Streams {

    private Map<String, FlinkKafkaConsumer011<Row>> streams = new HashMap<>();
    private LinkedHashMap<String, String> protoClassForTable = new LinkedHashMap<>();
    private static final String KAFKA_PREFIX = "kafka_consumer_config_";

    public Streams(String jsonArrayString, String rowTimeAttributeName) {
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

        return new FlinkKafkaConsumer011<>(Pattern.compile(topics), new ProtoDeserializer(protoClassName, timestampFieldIndex, rowTimeAttributeName), kafkaProps);
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
