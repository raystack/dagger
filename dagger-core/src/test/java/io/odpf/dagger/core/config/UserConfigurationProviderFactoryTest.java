package io.odpf.dagger.core.config;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;


public class UserConfigurationProviderFactoryTest {


    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setUp() {
        System.setProperty("key", "envValue");

    }

    @Test
    public void shouldProvideFromEnvironmentBasedOnConfigProperty() {
        UserConfigurationProviderFactory providerFactory = new UserConfigurationProviderFactory(new String[]{"--key", "argValue"});
        environmentVariables.set("key", "envValue");
        System.setProperty("ConfigSource", "ENVIRONMENT");

        UserConfigurationProvider provider = providerFactory.provider();

        assertEquals("envValue", provider.getUserConf().getParam().get("key", ""));
        assertThat(provider, instanceOf(EnvironmentConfigurationProvider.class));

    }

    @Test
    public void shouldProvideFromCommandline() {
        System.setProperty("ConfigSource", "ARGS");
        UserConfigurationProviderFactory providerFactory = new UserConfigurationProviderFactory(new String[]{"--key", "isargValue"});
        UserConfigurationProvider provider = providerFactory.provider();

        assertEquals("isargValue", provider.getUserConf().getParam().get("key", ""));
        assertThat(provider, instanceOf(CommandlineConfigurationProvider.class));
    }

    @Test
    public void shoulDProvideFileBasedConfiguration() {
        System.setProperty("ConfigSource", "FILE");
        System.setProperty("DAGGER_CONFIG_PATH", "env/local.properties");
        UserConfigurationProviderFactory providerFactory = new UserConfigurationProviderFactory(new String[]{"--key", "argValue"});
        UserConfigurationProvider provider = providerFactory.provider();

        assertThat(provider, instanceOf(FileConfigurationProvider.class));
    }
}
