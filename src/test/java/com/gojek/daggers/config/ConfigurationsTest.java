package com.gojek.daggers.config;

import com.gojek.daggers.config.commandline.CommandlineConfigurationProvider;
import com.gojek.daggers.config.system.EnvironmentConfigurationProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationsTest {

    @Test
    public void shouldProvideFromEnvironment() {
        System.setProperty("key", "value");

        Map<String, String> conf = Configurations.from(EnvironmentConfigurationProvider.class.getName(), null);

        assertTrue(conf.containsKey("key"));
        assertTrue(conf.get("key").equals("value"));
    }

    @Test
    public void shouldProvideFromCommandline() {

        Map<String, String> conf = Configurations.from(CommandlineConfigurationProvider.class.getName(), "--key value");

        assertTrue(conf.containsKey("key"));
        assertTrue(conf.get("key").equals("value"));
    }
}
