package io.odpf.dagger.functions.transformers;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(MockitoJUnitRunner.class)
public class DeDuplicationTransformerTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private Configuration configuration;

    @Mock
    private org.apache.flink.configuration.Configuration flinkInternalConfig;

    @Mock
    private MapState<String, Integer> mapState;

    @Mock
    private DataStream<Row> dataStream;

    @Mock
    private KeyedStream<Row, Object> keyedStream;


    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldGetMapStateFromRuntimeContext() throws Exception {
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("key_column", "status");
        transformationArguments.put("ttl_in_seconds", 10);
        String[] columnNames = {"order_number", "service_type", "status"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "123");
        inputRow.setField(1, "TEST_SERVICE_TYPE");
        inputRow.setField(2, "TEST_STATUS");
        DeDuplicationTransformerStub deDuplicationTransformerStub = new DeDuplicationTransformerStub(transformationArguments, columnNames);
        deDuplicationTransformerStub.open(flinkInternalConfig);

        verify(runtimeContext, times(1)).getMapState(any(MapStateDescriptor.class));
    }

    @Test
    public void shouldNotFilterRecordsIfKeyNotPresentAndInsertInMap() throws Exception {
        when(runtimeContext.getMapState(any(MapStateDescriptor.class))).thenReturn(mapState);
        when(mapState.contains("TEST_STATUS")).thenReturn(false);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("key_column", "status");
        transformationArguments.put("ttl_in_seconds", 10);
        String[] columnNames = {"order_number", "service_type", "status"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "123");
        inputRow.setField(1, "TEST_SERVICE_TYPE");
        inputRow.setField(2, "TEST_STATUS");
        DeDuplicationTransformerStub deDuplicationTransformerStub = new DeDuplicationTransformerStub(transformationArguments, columnNames);
        deDuplicationTransformerStub.open(flinkInternalConfig);

        Assert.assertTrue(deDuplicationTransformerStub.filter(inputRow));
        verify(mapState, times(1)).put("TEST_STATUS", 1);
    }

    @Test
    public void shouldFilterRecordsIfKeyAlreadyPresent() throws Exception {
        when(runtimeContext.getMapState(any(MapStateDescriptor.class))).thenReturn(mapState);
        when(mapState.contains("TEST_STATUS")).thenReturn(false).thenReturn(true);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("key_column", "status");
        transformationArguments.put("ttl_in_seconds", 10);
        String[] columnNames = {"order_number", "service_type", "status"};
        Row inputRow1 = new Row(3);
        inputRow1.setField(0, "123");
        inputRow1.setField(1, "TEST_SERVICE_TYPE");
        inputRow1.setField(2, "TEST_STATUS");
        Row inputRow2 = new Row(3);
        inputRow2.setField(0, "456");
        inputRow2.setField(1, "TEST_SERVICE_TYPE");
        inputRow2.setField(2, "TEST_STATUS");
        DeDuplicationTransformerStub deDuplicationTransformerStub = new DeDuplicationTransformerStub(transformationArguments, columnNames);
        deDuplicationTransformerStub.open(flinkInternalConfig);

        Assert.assertTrue(deDuplicationTransformerStub.filter(inputRow1));
        Assert.assertFalse(deDuplicationTransformerStub.filter(inputRow2));
        verify(mapState, times(1)).put("TEST_STATUS", 1);
    }

    @Test
    public void shouldTransformInputStreamWithKeyByAndApplyFilterFunction() {
        when(dataStream.keyBy(any(KeySelector.class))).thenReturn(keyedStream);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("key_column", "status");
        transformationArguments.put("ttl_in_seconds", 10);
        String[] columnNames = {"order_number", "service_type", "status"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "123");
        inputRow.setField(1, "TEST_SERVICE_TYPE");
        inputRow.setField(2, "TEST_STATUS");
        DeDuplicationTransformer deDuplicationTransformer = new DeDuplicationTransformer(transformationArguments, columnNames, configuration);
        StreamInfo inputStreamInfo = new StreamInfo(dataStream, columnNames);
        deDuplicationTransformer.transform(inputStreamInfo);
        verify(keyedStream, times(1)).filter(any(DeDuplicationTransformer.class));
    }

    @Test
    public void shouldReturnSameColumnNames() {
        when(dataStream.keyBy(any(KeySelector.class))).thenReturn(keyedStream);
        HashMap<String, Object> transformationArguments = new HashMap<>();
        transformationArguments.put("key_column", "status");
        transformationArguments.put("ttl_in_seconds", 10);
        String[] columnNames = {"order_number", "service_type", "status"};
        Row inputRow = new Row(3);
        inputRow.setField(0, "123");
        inputRow.setField(1, "TEST_SERVICE_TYPE");
        inputRow.setField(2, "TEST_STATUS");
        DeDuplicationTransformer deDuplicationTransformer = new DeDuplicationTransformer(transformationArguments, columnNames, configuration);
        StreamInfo inputStreamInfo = new StreamInfo(dataStream, columnNames);
        StreamInfo outputStreamInfo = deDuplicationTransformer.transform(inputStreamInfo);
        Assert.assertArrayEquals(columnNames, outputStreamInfo.getColumnNames());
    }

    public class DeDuplicationTransformerStub extends DeDuplicationTransformer {

        public DeDuplicationTransformerStub(Map<String, Object> transformationArguments, String[] columnNames) {
            super(transformationArguments, columnNames, configuration);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }
    }

}
