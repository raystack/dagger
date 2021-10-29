package io.odpf.dagger.core.config;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;


public class ConfigurationProviderFactoryTest {


    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setUp() {
        System.setProperty("key", "envValue");

    }

    @Test
    public void shouldProvideFromEnvironmentBasedOnConfigProperty() {
        ConfigurationProviderFactory providerFactory = new ConfigurationProviderFactory(new String[]{"--key", "argValue"});
        environmentVariables.set("key", "envValue");
        System.setProperty("ConfigSource", "ENVIRONMENT");

        ConfigurationProvider provider = providerFactory.provider();

        assertEquals("envValue", provider.get().getString("key", ""));
        assertThat(provider, instanceOf(EnvironmentConfigurationProvider.class));

    }

    @Test
    public void shouldProvideFromCommandline() {
        System.setProperty("ConfigSource", "ARGS");
        ConfigurationProviderFactory providerFactory = new ConfigurationProviderFactory(new String[]{"--key", "isargValue"});
        ConfigurationProvider provider = providerFactory.provider();

        assertEquals("isargValue", provider.get().getString("key", ""));
        assertThat(provider, instanceOf(CommandlineConfigurationProvider.class));
    }

    @Test
    public void shoulDProvideFileBasedConfiguration() {
        System.setProperty("ConfigSource", "FILE");
        System.setProperty("DAGGER_CONFIG_PATH", "env/local.properties");
        ConfigurationProviderFactory providerFactory = new ConfigurationProviderFactory(new String[]{"--key", "argValue"});
        ConfigurationProvider provider = providerFactory.provider();

        assertThat(provider, instanceOf(FileConfigurationProvider.class));
    }
}
