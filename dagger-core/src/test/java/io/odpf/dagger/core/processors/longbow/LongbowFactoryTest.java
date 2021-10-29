package io.odpf.dagger.core.processors.longbow;

import org.apache.flink.streaming.api.datastream.DataStream;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.longbow.processor.LongbowReader;
import io.odpf.dagger.core.processors.longbow.processor.LongbowWriter;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.PostProcessor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowFactoryTest {

    @Mock
    private StencilClientOrchestrator stencilClientOrchestrator;

    @Mock
    private Configuration configuration;

    @Mock
    private MetricsTelemetryExporter metricsTelemetryExporter;

    @Mock
    private AsyncProcessor asyncProcessor;

    @Mock
    private StreamInfo streamInfo;

    @Mock
    private DataStream dataStream;

    @Before
    public void setup() {
        initMocks(this);
        when(streamInfo.getDataStream()).thenReturn(dataStream);
    }

    @Test
    public void shouldReturnLongbowProcessorWithWriteOnly() {
        String[] inputColumnNames = new String[]{"longbow_write_key", "longbow_write", "rowtime", "event_timestamp"};
        when(streamInfo.getColumnNames()).thenReturn(inputColumnNames);
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[{\"INPUT_SCHEMA_PROTO_CLASS\": \"InputProtoMessage\"}]");
        LongbowSchema longbowSchema = new LongbowSchema(inputColumnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter, asyncProcessor);
        PostProcessor longbowProcessor = longbowFactory.getLongbowProcessor();
        StreamInfo outputStream = longbowProcessor.process(streamInfo);
        verify(asyncProcessor, times(1)).orderedWait(eq(dataStream), any(LongbowWriter.class), anyLong(), any(TimeUnit.class), anyInt());
        String[] expectedColumnNames = {"longbow_write_key",
                "longbow_write",
                "rowtime",
                "event_timestamp",
                "bigtable_table_id",
                "input_class_name",
                "longbow_read_key"};
        assertArrayEquals(expectedColumnNames, outputStream.getColumnNames());
    }

    @Test
    public void shouldReturnLongbowProcessorWithReadOnly() {
        String[] inputColumnNames = new String[]{"longbow_read_key", "rowtime", "longbow_duration", "event_timestamp"};
        when(streamInfo.getColumnNames()).thenReturn(inputColumnNames);
        LongbowSchema longbowSchema = new LongbowSchema(inputColumnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter, asyncProcessor);
        PostProcessor longbowProcessor = longbowFactory.getLongbowProcessor();
        StreamInfo outputStream = longbowProcessor.process(streamInfo);
        verify(asyncProcessor, times(1)).orderedWait(eq(dataStream), any(LongbowReader.class), anyLong(), any(TimeUnit.class), anyInt());
        String[] expectedStringArr = new String[]{"longbow_read_key", "rowtime", "longbow_duration", "event_timestamp", "proto_data"};
        assertArrayEquals(expectedStringArr, outputStream.getColumnNames());
    }

    @Test
    public void shouldReturnLongbowProcessorWithReadAndWrite() {
        String[] inputColumnNames = new String[]{"longbow_key", "longbow_data", "rowtime", "event_timestamp", "longbow_duration"};
        when(streamInfo.getColumnNames()).thenReturn(inputColumnNames);
        LongbowSchema longbowSchema = new LongbowSchema(inputColumnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter, asyncProcessor);
        PostProcessor longbowProcessor = longbowFactory.getLongbowProcessor();
        StreamInfo outputStream = longbowProcessor.process(streamInfo);
        verify(asyncProcessor, times(1)).orderedWait(any(), any(LongbowReader.class), anyLong(), any(TimeUnit.class), anyInt());
        verify(asyncProcessor, times(1)).orderedWait(any(), any(LongbowWriter.class), anyLong(), any(TimeUnit.class), anyInt());
        assertEquals(inputColumnNames.length, outputStream.getColumnNames().length);
    }
}
