package com.gojek.daggers.postprocessor;

import com.gojek.daggers.DaggerConfigurationException;
import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.longbow.LongBowReader;
import com.gojek.daggers.longbow.LongBowSchema;
import com.gojek.daggers.longbow.LongBowWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongBowProcessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private Configuration configuration;
    @Mock
    private DataStream<Row> dataStream;
    @Mock
    private AsyncProcessor asyncProcessor;

    @Before
    public void setup() {
        initMocks(this);
        when(dataStream.getExecutionEnvironment()).thenReturn(mock(StreamExecutionEnvironment.class));
    }

    @Test
    public void shouldNotProcessDataStreamWhenDataFieldIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'longbow_data'");

        String[] columnNames = {"rowtime", "longbow_key", "longbow_duration", "event_timestamp"};
        final LongBowSchema longBowSchema = new LongBowSchema(columnNames);
        LongBowProcessor longBowProcessor = new LongBowProcessor(new LongBowWriter(configuration, longBowSchema), new LongBowReader(configuration, longBowSchema), asyncProcessor, longBowSchema);

        longBowProcessor.process(new StreamInfo(dataStream, columnNames));
        Mockito.verify(asyncProcessor, never()).orderedWait(any(), any(), any(), any(), anyInt());
    }

    @Test
    public void shouldNotProcessDataStreamWhenLongbowDurationFieldIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'longbow_duration'");

        String[] columnNames = {"rowtime", "longbow_key", "longbow_data1", "event_timestamp"};
        LongBowSchema longBowSchema = new LongBowSchema(columnNames);
        LongBowProcessor longBowProcessor = new LongBowProcessor(new LongBowWriter(configuration, longBowSchema), new LongBowReader(configuration, longBowSchema), asyncProcessor, longBowSchema);

        longBowProcessor.process(new StreamInfo(dataStream, columnNames));
        Mockito.verify(asyncProcessor, never()).orderedWait(any(), any(), any(), any(), anyInt());
    }

    @Test
    public void shouldNotProcessDataStreamWhenEventTimestampIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'event_timestamp'");

        String[] columnNames = {"rowtime", "longbow_key", "longbow_duration", "longbow_data1"};
        LongBowSchema longBowSchema = new LongBowSchema(columnNames);
        LongBowProcessor longBowProcessor = new LongBowProcessor(new LongBowWriter(configuration, longBowSchema), new LongBowReader(configuration, longBowSchema), asyncProcessor, longBowSchema);

        longBowProcessor.process(new StreamInfo(dataStream, columnNames));
        Mockito.verify(asyncProcessor, never()).orderedWait(any(), any(), any(), any(), anyInt());
    }

    @Test
    public void shouldNotProcessDataStreamWhenRowtimeIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'rowtime'");

        String[] columnNames = {"longbow_data1", "longbow_key", "longbow_duration", "event_timestamp"};
        LongBowSchema longBowSchema = new LongBowSchema(columnNames);
        LongBowProcessor longBowProcessor = new LongBowProcessor(new LongBowWriter(configuration, longBowSchema), new LongBowReader(configuration, longBowSchema), asyncProcessor, longBowSchema);

        longBowProcessor.process(new StreamInfo(dataStream, columnNames));
        Mockito.verify(asyncProcessor, never()).orderedWait(any(), any(), any(), any(), anyInt());
    }

    @Test
    public void shouldNotProcessDataStreamWhenMultipleFieldsAreMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'event_timestamp,rowtime'");

        String[] columnNames = {"longbow_data1", "longbow_key", "longbow_duration"};
        LongBowSchema longBowSchema = new LongBowSchema(columnNames);
        LongBowProcessor longBowProcessor = new LongBowProcessor(new LongBowWriter(configuration, longBowSchema), new LongBowReader(configuration, longBowSchema), asyncProcessor, longBowSchema);

        longBowProcessor.process(new StreamInfo(dataStream, columnNames));
        Mockito.verify(asyncProcessor, never()).orderedWait(any(), any(), any(), any(), anyInt());
    }

    @Test
    public void shouldChainFunctionsWhenAllFieldsPresentInQuery() {
        String[] columnNames = {"rowtime", "longbow_key", "longbow_duration", "event_timestamp", "longbow_data1"};
        LongBowSchema longBowSchema = new LongBowSchema(columnNames);
        LongBowWriter longbowWriter = new LongBowWriter(configuration, longBowSchema);
        LongBowReader longbowReader = new LongBowReader(configuration, longBowSchema);
        DataStream<Row> writerStream = mock(DataStream.class);
        DataStream<Row> readerStream = mock(DataStream.class);
        when(asyncProcessor.orderedWait(dataStream, longbowWriter, 5000, TimeUnit.MILLISECONDS, 40)).thenReturn(writerStream);
        when(asyncProcessor.orderedWait(writerStream, longbowReader, 5000, TimeUnit.MILLISECONDS, 40)).thenReturn(readerStream);
        LongBowProcessor longBowProcessor = new LongBowProcessor(longbowWriter, longbowReader, asyncProcessor, longBowSchema);

        longBowProcessor.process(new StreamInfo(dataStream, columnNames));

        Mockito.verify(asyncProcessor, times(1)).orderedWait(dataStream, longbowWriter, 5000, TimeUnit.MILLISECONDS, 40);
        Mockito.verify(asyncProcessor, times(1)).orderedWait(writerStream, longbowReader, 5000, TimeUnit.MILLISECONDS, 40);
    }
}
