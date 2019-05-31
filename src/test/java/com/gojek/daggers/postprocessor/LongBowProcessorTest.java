package com.gojek.daggers.postprocessor;

import com.gojek.daggers.DaggerConfigurationException;
import com.gojek.daggers.StreamInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareOnlyThisForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.Constants.SQL_QUERY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareOnlyThisForTest(AsyncDataStream.class)
public class LongBowProcessorTest {

    @Mock
    private Configuration configuration;

    @Mock
    private DataStream dataStream;

    @Mock
    private Table table;

    @Mock
    private TableSchema tableSchema;

    @Before
    public void setup(){
        PowerMockito.mockStatic(AsyncDataStream.class);

        when(AsyncDataStream.orderedWait(eq(dataStream), any(AsyncFunction.class), any(Long.class), any(TimeUnit.class), any(Integer.class))).thenReturn(mock(SingleOutputStreamOperator.class));
        when(table.getSchema()).thenReturn(tableSchema);
        when(tableSchema.getColumnNames()).thenReturn(new String[]{"rowtime", "longbow_key", "longbow_duration", "longbow_data1"});
        when(dataStream.getExecutionEnvironment()).thenReturn(mock(StreamExecutionEnvironment.class));
    }

    @Test
    public void shouldProcessDataStreamWhenAllRequiredFieldsArePresentInQuery() {
        String validSQL =  "SELECT rowtime, CONCAT('rule123#driver', driver_id) AS longbow_key, " +
                "'1d' AS longbow_duration, " +
                "order_number AS longbow_data1 " +
                "FROM booking " +
                "WHERE status='COMPLETED'";
        when(configuration.getString(SQL_QUERY, "")).thenReturn(validSQL);

        LongBowProcessor longBowProcessor = new LongBowProcessor(configuration, table.getSchema().getColumnNames());
        StreamInfo streamInfo = new StreamInfo(dataStream, table.getSchema().getColumnNames());
        StreamInfo resultStream = longBowProcessor.process(streamInfo);

        Assert.assertArrayEquals(streamInfo.getColumnNames(), resultStream.getColumnNames());
    }


    @Test(expected = DaggerConfigurationException.class)
    public void shouldNotProcessDataStreanWhenMissingRequiredFieldsInQuery() {
        String invalidSQL =  "SELECT rowtime, CONCAT('rule123#driver', driver_id) AS longbow_key, " +
                "'1d' AS longbow_duration, " +
                "FROM booking " +
                "WHERE status='COMPLETED'";
        when(configuration.getString(SQL_QUERY, "")).thenReturn(invalidSQL);

        LongBowProcessor longBowProcessor = new LongBowProcessor(configuration, table.getSchema().getColumnNames());
        StreamInfo streamInfo = new StreamInfo(dataStream, table.getSchema().getColumnNames());
        longBowProcessor.process(streamInfo);
    }
}
