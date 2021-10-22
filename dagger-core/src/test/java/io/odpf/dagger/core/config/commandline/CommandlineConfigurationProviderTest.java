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
        Configuration userConf = new CommandlineConfigurationProvider(new String[]{}).getUserConf();

        assertTrue(userConf.getParam().getConfiguration().keySet().isEmpty());
    }

    @Test
    public void shouldProvideFromOneValidInput() throws Exception {
        Configuration userConf = new CommandlineConfigurationProvider(new String[]{"--key", "value"}).getUserConf();

        assertEquals(1, userConf.getParam().getConfiguration().keySet().size());

        assertTrue(userConf.getParam().getConfiguration().containsKey("key"));
        assertEquals("value", userConf.getString("key", ""));
    }

    @Test
    public void shouldProvideFromMultipleValidInputs() throws Exception {
        Configuration userConf = new CommandlineConfigurationProvider(new String[]{"--key", "value", "--k", "v"}).getUserConf();

        assertEquals(2, userConf.getParam().getConfiguration().keySet().size());

        assertTrue(userConf.getParam().getConfiguration().containsKey("key"));
        assertEquals("value", userConf.getString("key", ""));
        assertTrue(userConf.getParam().getConfiguration().containsKey("k"));
        assertEquals("v", userConf.getString("k", ""));
    }

    @Test
    public void shouldUseEncodedArgsIfProvided() {
        String args = Base64.getEncoder().encodeToString("[\"--key\", \"value\"]".getBytes());
        Configuration userConf = new CommandlineConfigurationProvider(new String[]{"--encodedArgs", args}).getUserConf();

        assertTrue(userConf.getParam().getConfiguration().containsKey("key"));
        assertEquals("value", userConf.getString("key", ""));
    }
}
