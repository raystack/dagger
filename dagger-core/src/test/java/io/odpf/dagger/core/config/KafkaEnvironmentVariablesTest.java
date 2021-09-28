package io.odpf.dagger.core.config;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaEnvironmentVariablesTest {

    @Test
    public void shouldReturnKafkaEnvVarsPositive() {
        Configuration configuration = new Configuration();
        configuration.setString("PATH", "/usr/local/bin");
        configuration.setString("SHELL", "/usr/local/bin/zsh");
        configuration.setString("source_kafka_config_fetch_min_bytes", "1");
        configuration.setString("source_kafka_config_ssl_keystore_location", "/home/user/.ssh/keystore");
        configuration.setString("source_kafka_config_enable_auto_commit", "false");

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
        configuration.setString("PATH", "/usr/local/bin");
        configuration.setString("SHELL", "/usr/local/bin/zsh");

        Properties actualEnvVars = KafkaEnvironmentVariables.parse(configuration);

        assertTrue(actualEnvVars.isEmpty());
    }

    @Test
    public void shouldReturnEmptyCollectionOnNullConfiguration() {
        Configuration configuration = null;

        Properties actualEnvVars = KafkaEnvironmentVariables.parse(configuration);

        assertTrue(actualEnvVars.isEmpty());
    }

    @Test
    public void shouldReturnEmptyCollectionOnEmptyConfiguration() {
        Configuration configuration = new Configuration();

        Properties actualEnvVars = KafkaEnvironmentVariables.parse(configuration);

        assertTrue(actualEnvVars.isEmpty());
    }
}
