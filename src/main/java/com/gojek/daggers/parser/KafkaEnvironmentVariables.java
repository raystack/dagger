package com.gojek.daggers.parser;

import org.apache.flink.configuration.Configuration;

import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaEnvironmentVariables {

    private static final String KAFKA_PREFIX = "kafka_consumer_config_";

    public static Properties parse(Configuration configuration) {
        Properties props = new Properties();

        if (configuration == null || configuration.keySet().size() == 0) {
            return props;
        }

        configuration.toMap().entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().startsWith(KAFKA_PREFIX))
                .forEach( e -> props.setProperty(parseVarName(e.getKey()), e.getValue()));
        return props;
    }

    private static String parseVarName(String varName) {
        String[] names = varName.toLowerCase().replaceAll(KAFKA_PREFIX, "").split("_");
        return String.join(".", names);
    }
}
