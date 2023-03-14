package com.gotocompany.dagger.core.processors;

import com.gotocompany.dagger.common.core.DaggerContextTestBase;
import com.gotocompany.dagger.core.processors.types.Preprocessor;
import com.gotocompany.dagger.core.utils.Constants;
import com.gotocompany.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PreProcessorFactoryTest extends DaggerContextTestBase {

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
        when(configuration.getBoolean(Constants.PROCESSOR_PREPROCESSOR_ENABLE_KEY, Constants.PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(Constants.PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        List<Preprocessor> preProcessors = PreProcessorFactory.getPreProcessors(daggerContext, "booking", metricsTelemetryExporter);
        assertEquals(1, preProcessors.size());
    }
}
