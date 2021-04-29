package io.odpf.dagger.core.config;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationsTest {

    private ConfigurationProviderFactory providerFactory;

    @Before
    public void setUp() {
        System.setProperty("key", "envValue");
        providerFactory = new ConfigurationProviderFactory(new String[]{"--key", "argValue"});
    }

    @Ignore
    @Test
    public void shouldProvideFromEnvironmentBasedOnConfigProperty() {
        System.setProperty("ConfigSource", "ENVIRONMENT");

        ConfigurationProvider provider = providerFactory.provider();

        assertEquals(provider.get().getString("key", ""), "envValue");

    }

    @Test
    public void shouldProvideFromCommandline() {
        System.setProperty("ConfigSource", "ARGS");

        ConfigurationProvider provider = providerFactory.provider();

        assertEquals(provider.get().getString("key", ""), "argValue");
    }
}
