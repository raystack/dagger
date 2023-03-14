package com.gotocompany.dagger.core.config.system;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.core.config.EnvironmentConfigurationProvider;
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

        Configuration configuration = new EnvironmentConfigurationProvider(environmentParameters).get();
        assertEquals("value", configuration.getString("key", ""));
        assertEquals("value2", configuration.getString("key2", ""));
    }
}
