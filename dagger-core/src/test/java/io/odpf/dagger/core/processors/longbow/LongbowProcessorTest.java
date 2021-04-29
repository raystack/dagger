package io.odpf.dagger.core.processors.longbow;

import io.odpf.dagger.core.processors.longbow.columnmodifier.ColumnModifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.core.StreamInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.ArrayList;
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
    private ColumnModifier columnModifier;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldChainRichAsyncFunctions() {
        String[] columnNames = {"rowtime", "longbow_key", "event_timestamp"};
        RichAsyncFunction asyncFunction1 = mock(RichAsyncFunction.class);
        RichAsyncFunction asyncFunction2 = mock(RichAsyncFunction.class);
        ArrayList<RichAsyncFunction<Row, Row>> richAsyncFunctions = new ArrayList<>();
        richAsyncFunctions.add(asyncFunction1);
        richAsyncFunctions.add(asyncFunction2);
        LongbowProcessor longbowProcessor = new LongbowProcessor(asyncProcessor, configuration, richAsyncFunctions, columnModifier);
        longbowProcessor.process(new StreamInfo(dataStream, columnNames));
        verify(asyncProcessor, times(2))
                .orderedWait(any(), any(), anyLong(), any(TimeUnit.class), anyInt());
    }
}
