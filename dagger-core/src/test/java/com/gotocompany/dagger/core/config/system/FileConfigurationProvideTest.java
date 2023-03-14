package com.gotocompany.dagger.core.config.system;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.core.config.FileConfigurationProvider;
import com.gotocompany.dagger.core.exception.DaggerConfigurationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class FileConfigurationProvideTest {

    @Test
    public void readFromAConfigurationFile() {

        System.setProperty("DAGGER_CONFIG_PATH", "env/local.properties");
        Configuration configuration = new FileConfigurationProvider().get();
        assertEquals("1", configuration.getString("FLINK_PARALLELISM", "1"));
    }

    @Test
    public void shouldThrowExceptionForFileNotfound() {
        System.setProperty("DAGGER_CONFIG_PATH", "dd");
        DaggerConfigurationException exception = assertThrows(DaggerConfigurationException.class,
                () -> new FileConfigurationProvider().get());
        assertEquals("Config source not provided", exception.getMessage());

    }
}
