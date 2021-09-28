package io.odpf.dagger.core.config.system;

import io.odpf.dagger.core.config.EnvironmentConfigurationProvider;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class EnvironmentConfigurationProviderTest {

    @Test
    public void shouldProvideSystemConfiguration() {
        HashMap<String, String> environmentParameters = new HashMap<String, String>() {{
            put("key", "value");
            put("key2", "value2");
        }};

        Configuration stringStringMap = new EnvironmentConfigurationProvider(environmentParameters).get();
        assertEquals("value", stringStringMap.getString("key", ""));
        assertEquals("value2", stringStringMap.getString("key2", ""));
    }
}
