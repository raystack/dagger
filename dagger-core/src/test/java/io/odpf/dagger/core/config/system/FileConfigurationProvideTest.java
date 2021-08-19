package io.odpf.dagger.core.config.system;

import io.odpf.dagger.core.config.FileConfigurationProvider;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import org.apache.flink.configuration.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class FileConfigurationProvideTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void readFromAConfigurationFile() {

        System.setProperty("DAGGER_CONFIG_PATH", "env/local.properties");
        Configuration stringStringMap = new FileConfigurationProvider().get();
        assertEquals("1", stringStringMap.getString("FLINK_PARALLELISM", "1"));
    }

    @Test
    public void shouldThrowExceptionForFileNotfound() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Config source not provided");
        System.setProperty("DAGGER_CONFIG_PATH", "dd");
        Configuration stringStringMap = new FileConfigurationProvider().get();
    }
}
