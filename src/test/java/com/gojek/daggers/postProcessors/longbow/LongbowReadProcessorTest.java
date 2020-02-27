package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.utils.Constants.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowReadProcessorTest {

    @Mock
    private LongbowReader longbowReader;

    @Mock
    private AsyncProcessor asyncProcessor;

    @Mock
    private Configuration configuration;

    @Mock
    private DataStream<Row> dataStream;

    @Before
    public void setup() {
        initMocks(this);
        when(configuration.getInteger(LONGBOW_THREAD_CAPACITY_KEY, LONGBOW_THREAD_CAPACITY_DEFAULT)).thenReturn(30);
        when(configuration.getLong(LONGBOW_ASYNC_TIMEOUT_KEY, LONGBOW_ASYNC_TIMEOUT_DEFAULT)).thenReturn(15000L);

    }

    @Test
    public void shouldReadDataFromBigTable() {
        String[] columnNames = {"longbow_read_key", "event_timestamp"};
        LongbowReadProcessor longbowReadProcessor = new LongbowReadProcessor(longbowReader, asyncProcessor, configuration);
        DataStream<Row> readStream = mock(DataStream.class);
        when(asyncProcessor.orderedWait(dataStream, longbowReader, 15000, TimeUnit.MILLISECONDS, 30))
                .thenReturn(readStream);
        StreamInfo streamInfo = longbowReadProcessor.process(new StreamInfo(dataStream, columnNames));
        verify(asyncProcessor, times(1)).orderedWait(dataStream, longbowReader, 15000L, TimeUnit.MILLISECONDS, 30);
        String[] expectedColumnNames = {"longbow_read_key", "event_timestamp", "proto_data"};

        Assert.assertArrayEquals(expectedColumnNames, streamInfo.getColumnNames());
    }
}