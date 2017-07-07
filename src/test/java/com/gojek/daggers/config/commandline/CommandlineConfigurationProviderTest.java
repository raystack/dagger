package com.gojek.daggers.config.commandline;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommandlineConfigurationProviderTest {

    @Test
    public void shouldProvideFromEmptyInput() throws Exception {
        Map<String, String> configurations = new CommandlineConfigurationProvider(new String[]{}).get();

        assertTrue(configurations.size() == 0);
    }

    @Test
    public void shouldProvideFromOneValidInput() throws Exception {
        Map<String, String> configurations = new CommandlineConfigurationProvider(new String[]{"--key", "value"}).get();

        assertEquals(1, configurations.size());

        assertTrue(configurations.containsKey("key"));
        assertTrue(configurations.get("key").equals("value"));
    }

    @Test
    public void shouldProvideFromMultipleValidInputs() throws Exception {
        Map<String, String> configurations = new CommandlineConfigurationProvider(new String[]{"--key", "value", "--k", "v"}).get();

        assertEquals(2, configurations.size());

        assertTrue(configurations.containsKey("key"));
        assertTrue(configurations.get("key").equals("value"));
        assertTrue(configurations.containsKey("k"));
        assertTrue(configurations.get("k").equals("v"));
    }
}