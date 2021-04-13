package io.odpf.dagger.processors;

import org.apache.flink.configuration.Configuration;

import com.jayway.jsonpath.InvalidJsonException;
import io.odpf.dagger.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.processors.types.Preprocessor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.List;

import static io.odpf.dagger.utils.Constants.PRE_PROCESSOR_CONFIG_KEY;
import static io.odpf.dagger.utils.Constants.PRE_PROCESSOR_ENABLED_DEFAULT;
import static io.odpf.dagger.utils.Constants.PRE_PROCESSOR_ENABLED_KEY;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PreProcessorFactoryTest {


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    Configuration configuration;
    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    String preProcessorConfigJson = "{\n" +
            " \"table_transformers\": [{\n" +
            "   \"table_name\": \"booking\",\n" +
            "   \"transformers\": [{\n" +
            "    \"transformation_class\": \"com.gojek.daggers.PreProcessorClass\"\n" +
            "   }, {\n" +
            "    \"transformation_class\": \"com.gojek.daggers.PreProcessorClass\",\n" +
            "    \"transformation_arguments\": {\n" +
            "     \"key\": \"value\"\n" +
            "    }\n" +
            "   }]\n" +
            "  },\n" +
            "  {\n" +
            "   \"table_name\": \"another_booking\",\n" +
            "   \"transformers\": [{\n" +
            "    \"transformation_class\": \"com.gojek.daggers.PreProcessorClass\"\n" +
            "   }]\n" +
            "  }\n" +
            " ]\n" +
            "}";

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnPreProcessors() {
        when(configuration.getBoolean(PRE_PROCESSOR_ENABLED_KEY, PRE_PROCESSOR_ENABLED_DEFAULT)).thenReturn(true);
        when(configuration.getString(PRE_PROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        Assert.assertNotNull(preProcessorConfig);
        List<Preprocessor> preProcessors = PreProcessorFactory.getPreProcessors(configuration, preProcessorConfig, "booking", metricsTelemetryExporter);
        Assert.assertEquals(1, preProcessors.size());
    }

    @Test
    public void shouldParseConfig() {
        when(configuration.getBoolean(PRE_PROCESSOR_ENABLED_KEY, PRE_PROCESSOR_ENABLED_DEFAULT)).thenReturn(true);
        when(configuration.getString(PRE_PROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        Assert.assertEquals(2, preProcessorConfig.getTableTransformers().size());
        Assert.assertEquals(2, preProcessorConfig.getTableTransformers().get(0).getTransformers().size());
        Assert.assertEquals("com.gojek.daggers.PreProcessorClass", preProcessorConfig.getTableTransformers().get(0).getTransformers().get(0).getTransformationClass());
    }

    @Test
    public void shouldThrowExceptionForInvalidJson() {
        when(configuration.getBoolean(PRE_PROCESSOR_ENABLED_KEY, PRE_PROCESSOR_ENABLED_DEFAULT)).thenReturn(true);
        when(configuration.getString(PRE_PROCESSOR_CONFIG_KEY, "")).thenReturn("blah");
        expectedException.expectMessage("Invalid JSON Given for PRE_PROCESSOR_CONFIG");
        expectedException.expect(InvalidJsonException.class);
        PreProcessorFactory.parseConfig(configuration);
    }

    @Test
    public void shouldNotParseConfigWhenDisabled() {
        when(configuration.getBoolean(PRE_PROCESSOR_ENABLED_KEY, PRE_PROCESSOR_ENABLED_DEFAULT)).thenReturn(false);
        when(configuration.getString(PRE_PROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorConfig preProcessorConfig = PreProcessorFactory.parseConfig(configuration);
        Assert.assertNull(preProcessorConfig);
    }
}
