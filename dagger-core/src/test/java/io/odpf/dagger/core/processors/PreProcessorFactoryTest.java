package io.odpf.dagger.core.processors;

import com.jayway.jsonpath.InvalidJsonException;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.Preprocessor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static io.odpf.dagger.core.utils.Constants.PROCESSOR_PREPROCESSOR_CONFIG_KEY;
import static io.odpf.dagger.core.utils.Constants.PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.PROCESSOR_PREPROCESSOR_ENABLE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PreProcessorFactoryTest {

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    private String preProcessorConfigJson = "{\n"
            + " \"table_transformers\": [{\n"
            + "   \"table_name\": \"booking\",\n"
            + "   \"transformers\": [{\n"
            + "    \"transformation_class\": \"PreProcessorClass\"\n"
            + "   }, {\n"
            + "    \"transformation_class\": \"PreProcessorClass\",\n"
            + "    \"transformation_arguments\": {\n"
            + "     \"key\": \"value\"\n"
            + "    }\n"
            + "   }]\n"
            + "  },\n"
            + "  {\n"
            + "   \"table_name\": \"another_booking\",\n"
            + "   \"transformers\": [{\n"
            + "    \"transformation_class\": \"PreProcessorClass\"\n"
            + "   }]\n"
            + "  }\n"
            + " ]\n"
            + "}";

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnPreProcessors() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        assertNotNull(preProcessorConfig);
        List<Preprocessor> preProcessors = PreProcessorFactory.getPreProcessors(configuration, preProcessorConfig, "booking", metricsTelemetryExporter);
        assertEquals(1, preProcessors.size());
    }

    @Test
    public void shouldParseConfig() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        assertEquals(2, preProcessorConfig.getTableTransformers().size());
        assertEquals(2, preProcessorConfig.getTableTransformers().get(0).getTransformers().size());
        assertEquals("PreProcessorClass", preProcessorConfig.getTableTransformers().get(0).getTransformers().get(0).getTransformationClass());
    }

    @Test
    public void shouldThrowExceptionForInvalidJson() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn("blah");
        InvalidJsonException exception = assertThrows(InvalidJsonException.class, () -> PreProcessorFactory.parseConfig(configuration));
        assertEquals("Invalid JSON Given for PROCESSOR_PREPROCESSOR_CONFIG", exception.getMessage());
    }

    @Test
    public void shouldNotParseConfigWhenDisabled() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(false);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        assertNull(preProcessorConfig);
    }
}
