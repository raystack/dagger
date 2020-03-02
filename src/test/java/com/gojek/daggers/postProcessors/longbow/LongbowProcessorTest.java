package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.core.StreamInfo;
import com.gojek.daggers.postProcessors.common.AsyncProcessor;
import com.gojek.daggers.postProcessors.longbow.data.LongbowData;
import com.gojek.daggers.postProcessors.longbow.outputRow.OutputLongbowData;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowReader;
import com.gojek.daggers.postProcessors.longbow.processor.LongbowWriter;
import com.gojek.daggers.postProcessors.longbow.request.PutRequestFactory;
import com.gojek.daggers.postProcessors.longbow.request.ScanRequestFactory;
import com.gojek.daggers.postProcessors.longbow.row.LongbowDurationRange;
import com.gojek.daggers.sink.ProtoSerializer;
import com.gojek.daggers.utils.Constants;
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

public class LongbowProcessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private Configuration configuration;
    @Mock
    private DataStream<Row> dataStream;
    @Mock
    private AsyncProcessor asyncProcessor;

    @Mock
    private LongbowDurationRange longbowDurationRow;

    @Mock
    private ProtoSerializer protoSerializer;

    @Mock
    private LongbowData longbowData;

    @Mock
    private ScanRequestFactory scanRequestFactory;

    private PutRequestFactory putRequestFactory;

    private String tableId;

    @Before
    public void setup() {
        initMocks(this);
        when(dataStream.getExecutionEnvironment()).thenReturn(mock(StreamExecutionEnvironment.class));
        when(longbowDurationRow.getInvalidFields()).thenReturn(new String[]{"longbow_earliest", "longbow_latest"});
        tableId = "test_table";
    }

    @Test
    public void shouldChainFunctionsWhenAllFieldsPresentInQuery() {
        String[] columnNames = {"rowtime", "longbow_key", "longbow_duration", "event_timestamp"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);
        putRequestFactory = new PutRequestFactory(longBowSchema, protoSerializer, tableId);
        LongbowWriter longbowWriter = new LongbowWriter(configuration, longBowSchema, putRequestFactory, tableId);
        OutputLongbowData outputLongbowData = new OutputLongbowData(longBowSchema);
        LongbowReader longbowReader = new LongbowReader(configuration, longBowSchema, longbowDurationRow, longbowData, scanRequestFactory, outputLongbowData);
        DataStream<Row> writerStream = mock(DataStream.class);
        DataStream<Row> readerStream = mock(DataStream.class);
        when(configuration.getLong(Constants.LONGBOW_ASYNC_TIMEOUT_KEY, Constants.LONGBOW_ASYNC_TIMEOUT_DEFAULT)).thenReturn(Constants.LONGBOW_ASYNC_TIMEOUT_DEFAULT);
        when(configuration.getInteger(Constants.LONGBOW_THREAD_CAPACITY_KEY, Constants.LONGBOW_THREAD_CAPACITY_DEFAULT)).thenReturn(Constants.LONGBOW_THREAD_CAPACITY_DEFAULT);
        when(asyncProcessor.orderedWait(dataStream, longbowWriter, 15000, TimeUnit.MILLISECONDS, 30)).thenReturn(writerStream);
        when(asyncProcessor.orderedWait(writerStream, longbowReader, 15000, TimeUnit.MILLISECONDS, 30)).thenReturn(readerStream);
        LongbowProcessor longBowProcessor = new LongbowProcessor(longbowWriter, longbowReader, asyncProcessor, configuration);

        longBowProcessor.process(new StreamInfo(dataStream, columnNames));

        Mockito.verify(asyncProcessor, times(1)).orderedWait(dataStream, longbowWriter, 15000, TimeUnit.MILLISECONDS, 30);
        Mockito.verify(asyncProcessor, times(1)).orderedWait(writerStream, longbowReader, 15000, TimeUnit.MILLISECONDS, 30);
    }
}
