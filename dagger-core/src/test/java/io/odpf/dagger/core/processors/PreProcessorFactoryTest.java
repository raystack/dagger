package io.odpf.dagger.core.processors;

import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.Preprocessor;
import org.apache.flink.configuration.Configuration;

import com.jayway.jsonpath.InvalidJsonException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.List;

import static io.odpf.dagger.core.utils.Constants.PROCESSOR_PREPROCESSOR_CONFIG_KEY;
import static io.odpf.dagger.core.utils.Constants.PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.PROCESSOR_PREPROCESSOR_ENABLE_KEY;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PreProcessorFactoryTest {


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private Configuration configuration;
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

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnPreProcessors() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        Assert.assertNotNull(preProcessorConfig);
        List<Preprocessor> preProcessors = PreProcessorFactory.getPreProcessors(configuration, preProcessorConfig, "booking", metricsTelemetryExporter);
        Assert.assertEquals(1, preProcessors.size());
    }

    @Test
    public void shouldParseConfig() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        Assert.assertEquals(2, preProcessorConfig.getTableTransformers().size());
        Assert.assertEquals(2, preProcessorConfig.getTableTransformers().get(0).getTransformers().size());
        Assert.assertEquals("PreProcessorClass", preProcessorConfig.getTableTransformers().get(0).getTransformers().get(0).getTransformationClass());
    }

    @Test
    public void shouldThrowExceptionForInvalidJson() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn("blah");
        expectedException.expectMessage("Invalid JSON Given for PROCESSOR_PREPROCESSOR_CONFIG");
        expectedException.expect(InvalidJsonException.class);
        PreProcessorFactory.parseConfig(configuration);
    }

    @Test
    public void shouldNotParseConfigWhenDisabled() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(false);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        Assert.assertNull(preProcessorConfig);
    }
}
