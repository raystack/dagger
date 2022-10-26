package io.odpf.dagger.core.processors;

import com.jayway.jsonpath.InvalidJsonException;
import io.odpf.dagger.common.core.DaggerContextTestBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.transformers.TableTransformConfig;
import io.odpf.dagger.core.processors.transformers.TransformConfig;
import io.odpf.dagger.core.processors.transformers.TransformProcessor;
import io.odpf.dagger.core.processors.types.Preprocessor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static io.odpf.dagger.core.utils.Constants.*;
import static io.odpf.dagger.core.utils.Constants.PROCESSOR_PREPROCESSOR_CONFIG_KEY;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PreProcessorOrchestratorTest extends DaggerContextTestBase {

    @Mock
    private MetricsTelemetryExporter exporter;

    @Mock
    private StreamInfo streamInfo;

    @Mock
    private DataStream<Row> stream;

    @Before
    public void setup() {
        initMocks(this);
    }

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

    @Test
    public void shouldGetProcessors() {
        PreProcessorConfig config = new PreProcessorConfig();
        List<TransformConfig> transformConfigs = new ArrayList<>();
        transformConfigs.add(new TransformConfig("InvalidRecordFilterTransformer", new HashMap<>()));
        TableTransformConfig ttc = new TableTransformConfig("test", transformConfigs);
        config.tableTransformers = Collections.singletonList(ttc);
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(daggerContext, exporter, "test");
        Mockito.when(streamInfo.getColumnNames()).thenReturn(new String[0]);
        Mockito.when(streamInfo.getDataStream()).thenReturn(stream);

        List<Preprocessor> processors = ppo.getProcessors();

        assertEquals(1, processors.size());
        assertEquals("test", ((TransformProcessor) processors.get(0)).getTableName());
    }

    @Test
    public void shouldNotGetProcessors() {
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(daggerContext, exporter, "test");
        Mockito.when(streamInfo.getColumnNames()).thenReturn(new String[0]);
        Mockito.when(streamInfo.getDataStream()).thenReturn(stream);

        List<Preprocessor> processors = ppo.getProcessors();

        assertEquals(0, processors.size());
    }

    @Test
    public void shouldNotNullConfig() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(daggerContext, exporter, "test");
        PreProcessorConfig preProcessorConfig = ppo.parseConfig(configuration);
        assertNotNull(preProcessorConfig);
    }

    @Test
    public void shouldParseConfig() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(daggerContext, exporter, "test");
        PreProcessorConfig preProcessorConfig = ppo.parseConfig(configuration);
        assertEquals(2, preProcessorConfig.getTableTransformers().size());
        assertEquals(2, preProcessorConfig.getTableTransformers().get(0).getTransformers().size());
        assertEquals("PreProcessorClass", preProcessorConfig.getTableTransformers().get(0).getTransformers().get(0).getTransformationClass());
    }

    @Test
    public void shouldThrowExceptionForInvalidJson() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(true);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn("blah");
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(daggerContext, exporter, "test");
        InvalidJsonException exception = assertThrows(InvalidJsonException.class, () -> ppo.parseConfig(configuration));
        assertEquals("Invalid JSON Given for PROCESSOR_PREPROCESSOR_CONFIG", exception.getMessage());
    }

    @Test
    public void shouldNotParseConfigWhenDisabled() {
        when(configuration.getBoolean(PROCESSOR_PREPROCESSOR_ENABLE_KEY, PROCESSOR_PREPROCESSOR_ENABLE_DEFAULT)).thenReturn(false);
        when(configuration.getString(PROCESSOR_PREPROCESSOR_CONFIG_KEY, "")).thenReturn(preProcessorConfigJson);
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(daggerContext, exporter, "test");
        PreProcessorConfig preProcessorConfig = ppo.parseConfig(configuration);
        assertNull(preProcessorConfig);
    }
}
