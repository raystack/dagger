package io.odpf.dagger.core.config.system;

import io.odpf.dagger.common.configuration.UserConfiguration;
import io.odpf.dagger.core.config.FileConfigurationProvider;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class FileConfigurationProvideTest {

    @Test
    public void readFromAConfigurationFile() {

        System.setProperty("DAGGER_CONFIG_PATH", "env/local.properties");
        UserConfiguration userConf = new FileConfigurationProvider().getUserConf();
        assertEquals("1", userConf.getParam().get("FLINK_PARALLELISM", "1"));
    }

    @Test
    public void shouldThrowExceptionForFileNotfound() {
        System.setProperty("DAGGER_CONFIG_PATH", "dd");
        DaggerConfigurationException exception = assertThrows(DaggerConfigurationException.class,
                () -> new FileConfigurationProvider().getUserConf());
        assertEquals("Config source not provided", exception.getMessage());

    }
}
