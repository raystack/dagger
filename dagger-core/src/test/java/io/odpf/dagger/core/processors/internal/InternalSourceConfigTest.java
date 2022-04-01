package io.odpf.dagger.core.processors.internal;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class InternalSourceConfigTest {
    @Test
    public void getMandatoryFieldsTest() {
        InternalSourceConfig config = new InternalSourceConfig("outputField1", "value1", "eurekatype", null);
        HashMap<String, Object> mandatoryFields = config.getMandatoryFields();
        Map<String, Object> expectedObjectMap = new HashMap<String, Object>() {{
            put("output_field", "outputField1");
            put("value", "value1");
            put("type", "eurekatype");
        }};

        assertEquals(expectedObjectMap, mandatoryFields);
    }

    @Test
    public void getTypeTest() {
        InternalSourceConfig config = new InternalSourceConfig("outputField1", "value1", "eurekatype", null);
        assertEquals("eurekatype", config.getType());
    }

    @Test
    public void getOutputFieldTest() {
        InternalSourceConfig config = new InternalSourceConfig("outputField1", "value1", "eurekatype", null);
        assertEquals("outputField1", config.getOutputField());
    }

    @Test
    public void getValueTest() {
        InternalSourceConfig config = new InternalSourceConfig("outputField1", "value1", "eurekatype", null);
        assertEquals("value1", config.getValue());
    }

    @Test
    public void getInternalProcessorConfigTest() {
        Map<String, String> internalProcessorConfig = new HashMap<String, String>();
        internalProcessorConfig.put("foo", "bar");

        InternalSourceConfig config = new InternalSourceConfig("outputField1", "value1", "eurekatype", internalProcessorConfig);
        assertEquals(internalProcessorConfig, config.getInternalProcessorConfig());
    }
}
