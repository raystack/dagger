package com.gojek.daggers.config;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationsTest {

    private ConfigurationProviderFactory providerFactory;

    @Before
    public void setUp(){
        System.setProperty("key", "envValue");
        providerFactory = new ConfigurationProviderFactory(new String[]{"--key", "argValue"});
    }

    @Test
    public void shouldProvideFromEnvironmentBasedOnConfigProperty() {
        System.setProperty("ConfigSource", "ENVIRONMENT");

        ConfigurationProvider provider = providerFactory.Provider();

        assertEquals(provider.get().get("key"), "envValue");

    }

    @Test
    public void shouldProvideFromCommandline() {
        System.setProperty("ConfigSource", "ARGS");

        ConfigurationProvider provider = providerFactory.Provider();

        assertEquals(provider.get().get("key"), "argValue");
    }
}
