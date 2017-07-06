package com.gojek.daggers.config.commandline;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommandlineConfigurationProviderTest {
    private CommandlineConfigurationProvider commandlineConfigurationProvider;

    @Before
    public void setUp() throws Exception {
        commandlineConfigurationProvider = new CommandlineConfigurationProvider();
    }

    @Test
    public void shouldProvideFromNullInput() throws Exception {
        Map<String, String> configurations = commandlineConfigurationProvider.get(null);

        assert configurations != null;
        assertTrue(configurations.size() == 0);
    }

    @Test
    public void shouldProvideFromEmptyInput() throws Exception {
        Map<String, String> configurations = commandlineConfigurationProvider.get("");

        assert configurations != null;
        assertTrue(configurations.size() == 0);
    }

    @Test
    public void shouldProvideFromOneValidInput() throws Exception {
        Map<String, String> configurations = commandlineConfigurationProvider.get("--key value");

        assertEquals(1, configurations.size());

        assertTrue(configurations.containsKey("key"));
        assertTrue(configurations.get("key").equals("value"));
    }

    @Test
    public void shouldProvideFromMultipleValidInputs() throws Exception {
        Map<String, String> configurations = commandlineConfigurationProvider.get("--key value --k v");

        assertEquals(2, configurations.size());

        assertTrue(configurations.containsKey("key"));
        assertTrue(configurations.get("key").equals("value"));
        assertTrue(configurations.containsKey("k"));
        assertTrue(configurations.get("k").equals("v"));
    }
}