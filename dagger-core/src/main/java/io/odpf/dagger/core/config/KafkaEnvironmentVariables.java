package io.odpf.dagger.core.config;

import org.apache.flink.configuration.Configuration;

import java.util.Properties;

/**
 * The type Kafka environment variables.
 */
public class KafkaEnvironmentVariables {

    private static final String KAFKA_PREFIX = "source_kafka_config_";

    /**
     * Parse properties.
     *
     * @param configuration the configuration
     * @return the properties
     */
    public static Properties parse(Configuration configuration) {
        Properties props = new Properties();

        if (configuration == null || configuration.keySet().size() == 0) {
            return props;
        }

        configuration.toMap().entrySet()
                .stream()
                .filter(e -> e.getKey().toLowerCase().startsWith(KAFKA_PREFIX))
                .forEach(e -> props.setProperty(parseVarName(e.getKey()), e.getValue()));
        return props;
    }

    private static String parseVarName(String varName) {
        String[] names = varName.toLowerCase().replaceAll(KAFKA_PREFIX, "").split("_");
        return String.join(".", names);
    }
}
