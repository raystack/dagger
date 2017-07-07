package com.gojek.daggers.config.system;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class EnvironmentConfigurationProviderTest {

    private EnvironmentConfigurationProvider environmentConfigurationProvider;

    @Before
    public void setup(){
        environmentConfigurationProvider = new EnvironmentConfigurationProvider();
    }

    @Test
    public void shouldProvideNonNullConfigurationMap() {

        Map<String, String> stringStringMap = environmentConfigurationProvider.get();

        assert stringStringMap != null;
    }

    @Test
    public void shouldProvideSystemConfiguration() {
        System.setProperty("key", "value");

        Map<String, String> stringStringMap = environmentConfigurationProvider.get();

        assertTrue(stringStringMap.containsKey("key"));
    }
}
