package io.odpf.dagger.common.configuration;

import org.apache.flink.api.java.utils.ParameterTool;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ConfigurationTest {

    @Mock
    private ParameterTool parameterTool;

    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        configuration = new Configuration(parameterTool);
    }

    @Test
    public void shouldGetStringFromParamTool() {
        when(parameterTool.get("test_config", "test_default")).thenReturn("test_value");

        assertEquals("test_value", configuration.getString("test_config", "test_default"));
    }

    @Test
    public void shouldGetIntegerFromParamTool() {
        when(parameterTool.getInt("test_config", 1)).thenReturn(2);

        assertEquals(Integer.valueOf(2), configuration.getInteger("test_config", 1));
    }

    @Test
    public void shouldGetBooleanFromParamTool() {
        when(parameterTool.getBoolean("test_config", false)).thenReturn(true);

        assertEquals(true, configuration.getBoolean("test_config", false));
    }

    @Test
    public void shouldGetLongFromParamTool() {
        when(parameterTool.getLong("test_config", 1L)).thenReturn(2L);

        assertEquals(Long.valueOf(2), configuration.getLong("test_config", 1L));
    }
}
