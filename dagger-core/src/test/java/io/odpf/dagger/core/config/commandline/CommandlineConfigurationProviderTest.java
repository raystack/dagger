package io.odpf.dagger.core.config.commandline;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.config.CommandlineConfigurationProvider;
import org.junit.Test;

import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommandlineConfigurationProviderTest {

    @Test
    public void shouldProvideFromEmptyInput() throws Exception {
        Configuration configuration = new CommandlineConfigurationProvider(new String[]{}).get();

        assertTrue(configuration.getParam().getConfiguration().keySet().isEmpty());
    }

    @Test
    public void shouldProvideFromOneValidInput() throws Exception {
        Configuration configuration = new CommandlineConfigurationProvider(new String[]{"--key", "value"}).get();

        assertEquals(1, configuration.getParam().getConfiguration().keySet().size());

        assertTrue(configuration.getParam().getConfiguration().containsKey("key"));
        assertEquals("value", configuration.getString("key", ""));
    }

    @Test
    public void shouldProvideFromMultipleValidInputs() throws Exception {
        Configuration configuration = new CommandlineConfigurationProvider(new String[]{"--key", "value", "--k", "v"}).get();

        assertEquals(2, configuration.getParam().getConfiguration().keySet().size());

        assertTrue(configuration.getParam().getConfiguration().containsKey("key"));
        assertEquals("value", configuration.getString("key", ""));
        assertTrue(configuration.getParam().getConfiguration().containsKey("k"));
        assertEquals("v", configuration.getString("k", ""));
    }

    @Test
    public void shouldUseEncodedArgsIfProvided() {
        String args = Base64.getEncoder().encodeToString("[\"--key\", \"value\"]".getBytes());
        Configuration configuration = new CommandlineConfigurationProvider(new String[]{"--encodedArgs", args}).get();

        assertTrue(configuration.getParam().getConfiguration().containsKey("key"));
        assertEquals("value", configuration.getString("key", ""));
    }
}
