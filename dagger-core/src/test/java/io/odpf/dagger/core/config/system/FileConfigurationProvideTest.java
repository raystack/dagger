package io.odpf.dagger.core.config.system;

import io.odpf.dagger.core.config.FileConfigurationProvider;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FileConfigurationProvideTest {

    @Test
    public void ReadFromAConfigurationFile() {

        System.setProperty("DAGGER_CONFIG_PATH", "env/local.properties");
        Configuration stringStringMap = new FileConfigurationProvider().get();

        assertEquals(stringStringMap.getString("PARALLELISM", "1"), "1");
    }

}
