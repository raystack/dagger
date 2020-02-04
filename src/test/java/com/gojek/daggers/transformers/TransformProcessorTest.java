package com.gojek.daggers.transformers;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import com.gojek.daggers.postProcessors.transfromers.TransformConfig;
import com.gojek.daggers.postProcessors.transfromers.TransformProcessor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class TransformProcessorTest {


    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private List<TransformConfig> transfromConfigs;

    @Mock
    private StreamInfo streamInfo;

    @Mock
    private DataStream<Row> dataStream;

    @Mock
    private SingleOutputStreamOperator mappedDataStream;

    @Mock
    private MapFunction<Row, Row> mockMapFunction;

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldThrowExceptionInCaseOfWrongClassName() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("wrongClassName");
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("wrongClassName", transformationArguments));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs);
        transformProcessor.process(streamInfo);
    }

    @Test
    public void shouldThrowExceptionInCaseOfWrongConstructorTypeSupported() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("com.gojek.daggers.postProcessors.transfromers.TransformProcessor.<init>(java.util.Map, [Ljava.lang.String;)");
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gojek.daggers.postProcessors.transfromers.TransformProcessor", transformationArguments));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs);
        transformProcessor.process(streamInfo);
    }

    @Test
    public void shouldProcessClassExtendingMapFunction() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessor = new TransformProcessorMock(mockMapFunction, transfromConfigs);
        transformProcessor.process(streamInfo);

        verify(dataStream, times(1)).map(mockMapFunction);
    }

    @Test
    public void shouldAddPostProcessorTypeMetrics() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");

        ArrayList<String> postProcessorType = new ArrayList<>();
        postProcessorType.add("transform_processor");
        HashMap<String, List<String>> metrics = new HashMap<>();
        metrics.put("post_processor_type", postProcessorType);
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessorMock = new TransformProcessorMock(mockMapFunction, transfromConfigs);
        transformProcessorMock.preProcessBeforeNotifyingSubscriber();

        Assert.assertEquals(metrics, transformProcessorMock.getTelemetry());
    }

    @Test
    public void shouldNotifySubscribers() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");

        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("MapClass", transformationArguments));

        TransformProcessorMock transformProcessorMock = new TransformProcessorMock(mockMapFunction, transfromConfigs);
        transformProcessorMock.notifySubscriber(metricsTelemetryExporter);

        verify(metricsTelemetryExporter, times(1)).updated(transformProcessorMock);
    }

    @Test
    public void shouldProcessMultiplePostTransformers() {
        when(streamInfo.getDataStream()).thenReturn(dataStream);
        when(streamInfo.getColumnNames()).thenReturn(null);
        when(dataStream.map(any(MapFunction.class))).thenReturn(mappedDataStream);
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gojek.dagger.transformer.FeatureTransformer", transformationArguments));
        transfromConfigs.add(new TransformConfig("com.gojek.dagger.transformer.ClearColumnTransformer", transformationArguments));

        TransformProcessor transformProcessor = new TransformProcessor(transfromConfigs);
        transformProcessor.process(streamInfo);

        verify(mappedDataStream, times(1)).map(any());
    }

    class TransformProcessorMock extends TransformProcessor {

        private MapFunction<Row, Row> mockMapFunction;

        public TransformProcessorMock(MapFunction<Row, Row> mockMapFunction, List<TransformConfig> transformConfigs) {
            super(transformConfigs);
            this.mockMapFunction = mockMapFunction;
        }

        protected MapFunction<Row, Row> getTransformMethod(TransformConfig transformConfig, String className, String[] columnNames) {
            return this.mockMapFunction;
        }
    }

}