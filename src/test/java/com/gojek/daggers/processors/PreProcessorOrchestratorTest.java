package com.gojek.daggers.processors;

import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.processors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.processors.transformers.TableTransformConfig;
import com.gojek.daggers.processors.transformers.TransformConfig;
import com.gojek.daggers.processors.transformers.TransformProcessor;
import com.gojek.daggers.processors.types.Preprocessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

public class PreProcessorOrchestratorTest {

    @Mock
    MetricsTelemetryExporter exporter;

    @Mock
    StreamInfo streamInfo;

    @Mock
    DataStream<Row> stream;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldGetProcessors() {
        Configuration configuration = new Configuration();
        PreProcessorConfig config = new PreProcessorConfig();
        List<TransformConfig> transformConfigs = new ArrayList<>();
        transformConfigs.add(new TransformConfig("com.gojek.dagger.transformer.filter.InvalidRecordFilterTransformer", new HashMap<>()));
        TableTransformConfig ttc = new TableTransformConfig("test", transformConfigs);
        config.tableTransformers = Collections.singletonList(ttc);
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(configuration, config, exporter, "test");
        Mockito.when(streamInfo.getColumnNames()).thenReturn(new String[0]);
        Mockito.when(streamInfo.getDataStream()).thenReturn(stream);

        List<Preprocessor> processors = ppo.getProcessors();

        Assert.assertEquals(1, processors.size());
        Assert.assertEquals("test", ((TransformProcessor) processors.get(0)).getTableName());
    }

    @Test
    public void shouldNotGetProcessors() {
        Configuration configuration = new Configuration();
        PreProcessorConfig config = new PreProcessorConfig();
        PreProcessorOrchestrator ppo = new PreProcessorOrchestrator(configuration, config, exporter, "test");
        Mockito.when(streamInfo.getColumnNames()).thenReturn(new String[0]);
        Mockito.when(streamInfo.getDataStream()).thenReturn(stream);

        List<Preprocessor> processors = ppo.getProcessors();

        Assert.assertEquals(0, processors.size());
    }
}
