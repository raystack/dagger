package io.odpf.dagger.core.config;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class KafkaEnvironmentVariablesTest {

    @Test
    public void shouldReturnKafkaEnvVarsPositive() {
        Configuration configuration = new Configuration();
        new HashMap<String, String>() {{
            put("PATH", "/usr/local/bin");
            put("SHELL", "/usr/local/bin/zsh");
            put("source_kafka_config_fetch_min_bytes", "1");
            put("source_kafka_config_ssl_keystore_location", "/home/user/.ssh/keystore");
            put("source_kafka_config_enable_auto_commit", "false");
        }}.entrySet()
                .stream()
                .forEach(e -> configuration.setString(e.getKey(), e.getValue()));

        Properties expectedEnvVars = new Properties() {{
            put("fetch.min.bytes", "1");
            put("ssl.keystore.location", "/home/user/.ssh/keystore");
            put("enable.auto.commit", "false");
        }};

        Properties actualEnvVars = KafkaEnvironmentVariables.parse(configuration);

        assertEquals(expectedEnvVars, actualEnvVars);
    }

    @Test
    public void shouldReturnKafkaEnvVarsNegative() {
        Configuration configuration = new Configuration();
        new HashMap<String, String>() {{
            put("PATH", "/usr/local/bin");
            put("SHELL", "/usr/local/bin/zsh");
        }}.entrySet()
                .stream()
                .forEach(e -> configuration.setString(e.getKey(), e.getValue()));

        Map<String, String> expectedEnvVars = new HashMap<>();

        Properties actualEnvVars = KafkaEnvironmentVariables.parse(configuration);

        assertEquals(expectedEnvVars, actualEnvVars);
    }

    @Test
    public void shouldReturnEmptyCollectionOnNullConfiguration() {
        Configuration configuration = null;
        Map<String, String> expectedEnvVars = new HashMap<>();

        Properties actualEnvVars = KafkaEnvironmentVariables.parse(configuration);

        assertEquals(expectedEnvVars, actualEnvVars);
    }

    @Test
    public void shouldReturnEmptyCollectionOnEmptyConfiguration() {
        Configuration configuration = new Configuration();

        Map<String, String> expectedEnvVars = new HashMap<>();

        Properties actualEnvVars = KafkaEnvironmentVariables.parse(configuration);

        assertEquals(expectedEnvVars, actualEnvVars);
    }
}
