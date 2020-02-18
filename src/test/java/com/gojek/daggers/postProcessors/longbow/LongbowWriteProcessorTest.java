package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowWriter;
import com.gojek.daggers.postProcessors.longbow.request.PutRequestFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.utils.Constants.*;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowWriteProcessorTest {

    @Mock
    private Configuration configuration;
    @Mock
    private AsyncProcessor asyncProcessor;
    @Mock
    private PutRequestFactory putRequestFactory;
    @Mock
    private DataStream<Row> dataStream;
    @Mock
    private LongbowWriter longbowWriter;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldWriteToBigTableAndAppendWithStaticData() {
        String[] columnNames = {"rowtime", "longbow_key", "longbow_duration", "event_timestamp"};
        when(configuration.getLong(LONGBOW_ASYNC_TIMEOUT_KEY, LONGBOW_ASYNC_TIMEOUT_DEFAULT)).thenReturn(15000L);
        when(configuration.getInteger(LONGBOW_THREAD_CAPACITY_KEY, LONGBOW_THREAD_CAPACITY_DEFAULT)).thenReturn(30);
        String inputProtoClassName = "com.gojek.esb.booking.BookingLogMessage";
        DataStream<Row> writerStream = mock(DataStream.class);
        when(asyncProcessor.orderedWait(dataStream, longbowWriter, 15000, TimeUnit.MILLISECONDS, 30))
                .thenReturn(writerStream);
        LongbowWriteProcessor longbowWriteProcessor = new LongbowWriteProcessor(longbowWriter, asyncProcessor,
                configuration, inputProtoClassName);

        StreamInfo streamInfo = longbowWriteProcessor.process(new StreamInfo(dataStream, columnNames));

        Mockito.verify(asyncProcessor, times(1)).orderedWait(dataStream, longbowWriter, 15000, TimeUnit.MILLISECONDS, 30);

        String[] expectedColumnNames = {"rowtime", "longbow_key", "longbow_duration", "event_timestamp",
                "bigtable_instance_id", "bigtable_project_id", "bigtable_table_name", "input_class_name"};
        assertArrayEquals(expectedColumnNames, streamInfo.getColumnNames());
    }
}
