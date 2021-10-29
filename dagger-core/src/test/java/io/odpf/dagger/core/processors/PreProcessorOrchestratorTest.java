package io.odpf.dagger.core.processors;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class PreProcessorOrchestratorTest {

    @Mock
    private MetricsTelemetryExporter exporter;

    @Mock
    private StreamInfo streamInfo;

    @Mock
    private DataStream<Row> stream;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldGetProcessors() {
        PreProcessorConfig config = new PreProcessorConfig();
        List<TransformConfig> transformConfigs = new ArrayList<>();
        transformConfigs.add(new TransformConfig("InvalidRecordFilterTransformer", new HashMap<>()));
        TableTransformConfig ttc = new TableTransformConfig("test", transformConfigs);
        config.tableTransformers = Collections.singletonList(ttc);
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(configuration, config, exporter, "test");
        Mockito.when(streamInfo.getColumnNames()).thenReturn(new String[0]);
        Mockito.when(streamInfo.getDataStream()).thenReturn(stream);

        List<Preprocessor> processors = ppo.getProcessors();

        assertEquals(1, processors.size());
        assertEquals("test", ((TransformProcessor) processors.get(0)).getTableName());
    }

    @Test
    public void shouldNotGetProcessors() {
        PreProcessorConfig config = new PreProcessorConfig();
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(configuration, config, exporter, "test");
        Mockito.when(streamInfo.getColumnNames()).thenReturn(new String[0]);
        Mockito.when(streamInfo.getDataStream()).thenReturn(stream);

        List<Preprocessor> processors = ppo.getProcessors();

        assertEquals(0, processors.size());
    }
}
