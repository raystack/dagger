package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.postprocessor.parser.TransformConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
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
    private MapFunction<Row, Row> mockMapFunction;

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
        expectedException.expectMessage("com.gojek.daggers.postprocessor.TransformProcessor.<init>(java.util.Map)");
        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("keyField", "keystore");
        transfromConfigs = new ArrayList<>();
        transfromConfigs.add(new TransformConfig("com.gojek.daggers.postprocessor.TransformProcessor", transformationArguments));

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

    class TransformProcessorMock extends TransformProcessor {

        private MapFunction<Row, Row> mockMapFunction;

        public TransformProcessorMock(MapFunction<Row, Row> mockMapFunction, List<TransformConfig> transformConfigs) {
            super(transformConfigs);
            this.mockMapFunction = mockMapFunction;
        }

        protected MapFunction<Row, Row> getTransformMethod(TransformConfig transformConfig, String className) {
            return this.mockMapFunction;
        }
    }

}