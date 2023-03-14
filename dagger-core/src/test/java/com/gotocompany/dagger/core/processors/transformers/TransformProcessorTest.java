package com.gotocompany.dagger.core.processors.transformers;

import com.gotocompany.dagger.common.core.DaggerContextTestBase;
import com.gotocompany.dagger.common.core.StreamInfo;
import com.gotocompany.dagger.common.core.Transformer;
import com.gotocompany.dagger.core.metrics.telemetry.TelemetryTypes;
import com.gotocompany.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.gotocompany.dagger.core.metrics.telemetry.TelemetryTypes.PRE_PROCESSOR_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class TransformProcessorTest extends DaggerContextTestBase {


    @Mock
    private List<TransformConfig> transfromConfigs;

    @Mock
    private StreamInfo streamInfo;

    @Mock
    private SingleOutputStreamOperator mappedDataStream;

    @Mock
    private Transformer transformer;

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldThrowExceptionInCaseOfWrongClassName() {
        when(streamInfo.getDataStream()).thenReturn(inputStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("wrongClassName", transformationArguments));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, daggerContext);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> transformProcessor.process(streamInfo));
        assertEquals("wrongClassName", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionInCaseOfWrongConstructorTypeSupported() {
        when(streamInfo.getDataStream()).thenReturn(inputStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gotocompany.dagger.core.processors.transformers.TransformProcessor", transformationArguments));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, daggerContext);
        RuntimeException exception = assertThrows(RuntimeException.class, () -> transformProcessor.process(streamInfo));
        assertEquals("com.gotocompany.dagger.core.processors.transformers.TransformProcessor.<init>(java.util.Map, [Ljava.lang.String;, com.gotocompany.dagger.common.core.DaggerContext)", exception.getMessage());
    }

    @Test
    public void shouldProcessClassExtendingMapFunction() {
        when(streamInfo.getDataStream()).thenReturn(inputStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessor = new TransformProcessorMock(transformer, transfromConfigs);
        transformProcessor.process(streamInfo);

        verify(transformer, times(1)).transform(streamInfo);
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        when(streamInfo.getDataStream()).thenReturn(inputStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");

        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("transform_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessorMock = new TransformProcessorMock(transformer, transfromConfigs);
        transformProcessorMock.preProcessBeforeNotifyingSubscriber();

        assertEquals(metrics, transformProcessorMock.getTelemetry());
    }

    @Test
    public void shouldAddPreProcessorTypeMetrics() {
        when(streamInfo.getDataStream()).thenReturn(inputStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");

        ArrayList<String> preProcessorType = new ArrayList<>();
        preProcessorType.add("test_table_transform_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("pre_processor_type", preProcessorType);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessorMock = new TransformProcessorMock("test_table", PRE_PROCESSOR_TYPE, transformer, transfromConfigs);
        transformProcessorMock.preProcessBeforeNotifyingSubscriber();

        assertEquals(metrics, transformProcessorMock.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        when(streamInfo.getDataStream()).thenReturn(inputStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");

        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessorMock = new TransformProcessorMock(transformer, transfromConfigs);
        transformProcessorMock.notifySubscriber(metricsTelemetryExporter);

        verify(metricsTelemetryExporter, times(1)).updated(transformProcessorMock);
    }

    @Test
    public void shouldProcessTwoPostTransformers() {
        when(streamInfo.getDataStream()).thenReturn(inputStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        when(inputStream.map(any(MapFunction.class))).thenReturn(mappedDataStream);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gotocompany.dagger.core.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));
        transfromConfigs.add(new TransformConfig("com.gotocompany.dagger.core.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, daggerContext);
        transformProcessor.process(streamInfo);
        verify(mappedDataStream, times(1)).map(any());
    }

    @Test
    public void shouldProcessMultiplePostTransformers() {
        when(streamInfo.getDataStream()).thenReturn(inputStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        when(inputStream.map(any(MapFunction.class))).thenReturn(mappedDataStream);
        when(mappedDataStream.map(any(MapFunction.class))).thenReturn(mappedDataStream);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gotocompany.dagger.core.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));
        transfromConfigs.add(new TransformConfig("com.gotocompany.dagger.core.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));
        transfromConfigs.add(new TransformConfig("com.gotocompany.dagger.core.processors.transformers.MockTransformer", new HashMap<String, Object>() {{
            put("keyField", "keystore");
        }}));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs, daggerContext);
        transformProcessor.process(streamInfo);

        verify(mappedDataStream, times(2)).map(any());
    }

    @Test
    public void shouldPopulateDefaultArguments() {
        TransformConfig config = new TransformConfig("com.gotocompany.TestProcessor", new HashMap<String, Object>() {{
            put("test-key", "test-value");
        }});
        TransformProcessor processor = new TransformProcessor("test_table", PRE_PROCESSOR_TYPE, Collections.singletonList(config), daggerContext);
        assertEquals("test_table", processor.tableName);
        assertEquals(PRE_PROCESSOR_TYPE, processor.type);
        assertEquals(1, processor.transformConfigs.size());
        assertEquals("com.gotocompany.TestProcessor", processor.transformConfigs.get(0).getTransformationClass());
        assertEquals("test_table", processor.transformConfigs.get(0).getTransformationArguments().get("table_name"));
        assertEquals("test-value", processor.transformConfigs.get(0).getTransformationArguments().get("test-key"));
    }

    static class TransformerMock implements Transformer {
        @Override
        public StreamInfo transform(StreamInfo streamInfo) {
            return null;
        }
    }

    final class TransformProcessorMock extends TransformProcessor {

        private Transformer mockMapFunction;

        private TransformProcessorMock(Transformer mockMapFunction, List<TransformConfig> transformConfigs) {
            super(transformConfigs, daggerContext);
            this.mockMapFunction = mockMapFunction;
        }

        private TransformProcessorMock(String table, TelemetryTypes type, Transformer mockMapFunction, List<TransformConfig> transformConfigs) {
            super(table, type, transformConfigs, daggerContext);
            this.mockMapFunction = mockMapFunction;
        }

        protected Transformer getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) {
            return this.mockMapFunction;
        }
    }

}
