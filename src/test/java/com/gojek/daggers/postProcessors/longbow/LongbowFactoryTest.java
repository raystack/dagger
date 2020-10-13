package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.dagger.common.StreamInfo;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.common.PostProcessor;
import com.gojek.daggers.postProcessors.telemetry.processor.MetricsTelemetryExporter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.utils.Constants.INPUT_STREAMS;
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
        when(configuration.getString(INPUT_STREAMS, "")).thenReturn("[{\"PROTO_CLASS_NAME\": \"com.gojek.esb.booking.BookingLogMessage\"}]");
        LongbowSchema longbowSchema = new LongbowSchema(inputColumnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter, asyncProcessor);
        PostProcessor longbowProcessor = longbowFactory.getLongbowProcessor();
        StreamInfo outputStream = longbowProcessor.process(streamInfo);
        verify(asyncProcessor, times(1)).orderedWait(any(), any(), anyLong(), any(TimeUnit.class), anyInt());
        Assert.assertEquals(inputColumnNames.length + 3, outputStream.getColumnNames().length);
    }

    @Test
    public void shouldReturnLongbowProcessorWithReadOnly() {
        String[] inputColumnNames = new String[]{"longbow_read_key", "rowtime", "longbow_duration", "event_timestamp"};
        when(streamInfo.getColumnNames()).thenReturn(inputColumnNames);
        LongbowSchema longbowSchema = new LongbowSchema(inputColumnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter, asyncProcessor);
        PostProcessor longbowProcessor = longbowFactory.getLongbowProcessor();
        StreamInfo outputStream = longbowProcessor.process(streamInfo);
        verify(asyncProcessor, times(1)).orderedWait(any(), any(), anyLong(), any(TimeUnit.class), anyInt());
        Assert.assertEquals(inputColumnNames.length + 1, outputStream.getColumnNames().length);
    }

    @Test
    public void shouldReturnLongbowProcessorWithReadAndWrite() {
        String[] inputColumnNames = new String[]{"longbow_key", "longbow_data", "rowtime", "event_timestamp", "longbow_duration"};
        when(streamInfo.getColumnNames()).thenReturn(inputColumnNames);
        LongbowSchema longbowSchema = new LongbowSchema(inputColumnNames);
        LongbowFactory longbowFactory = new LongbowFactory(longbowSchema, configuration, stencilClientOrchestrator, metricsTelemetryExporter, asyncProcessor);
        PostProcessor longbowProcessor = longbowFactory.getLongbowProcessor();
        StreamInfo outputStream = longbowProcessor.process(streamInfo);
        verify(asyncProcessor, times(2)).orderedWait(any(), any(), anyLong(), any(TimeUnit.class), anyInt());
        Assert.assertEquals(inputColumnNames.length, outputStream.getColumnNames().length);
    }
}
