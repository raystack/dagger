package com.gojek.daggers.postprocessor;

import com.gojek.daggers.DaggerConfigurationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LongBowProcessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private Configuration configuration;
    @Mock
    private DataStream<Row> dataStream;
    @Mock
    private Table table;
    @Mock
    private TableSchema tableSchema;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(table.getSchema()).thenReturn(tableSchema);
        when(dataStream.getExecutionEnvironment()).thenReturn(mock(StreamExecutionEnvironment.class));
    }

    @Test
    public void shouldNotProcessDataStreamWhenDataFieldIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'longbow_data'");


        when(tableSchema.getColumnNames()).thenReturn(new String[]{"rowtime", "longbow_key", "longbow_duration", "event_timestamp"});
//        when(configuration.getString(SQL_QUERY, "")).thenReturn(invalidSQL);

        LongBowProcessor longBowProcessor = new LongBowProcessor(configuration, table.getSchema().getColumnNames());
        longBowProcessor.validateQuery();
    }

    @Test
    public void shouldNotProcessDataStreamWhenLongbowDurationFieldIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'longbow_duration'");

        when(tableSchema.getColumnNames()).thenReturn(new String[]{"rowtime", "longbow_key", "longbow_data1", "event_timestamp"});

        LongBowProcessor longBowProcessor = new LongBowProcessor(configuration, table.getSchema().getColumnNames());
        longBowProcessor.validateQuery();
    }

    @Test
    public void shouldNotProcessDataStreamWhenEventTimestampIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'event_timestamp'");

        when(tableSchema.getColumnNames()).thenReturn(new String[]{"rowtime", "longbow_key", "longbow_duration", "longbow_data1"});

        LongBowProcessor longBowProcessor = new LongBowProcessor(configuration, table.getSchema().getColumnNames());
        longBowProcessor.validateQuery();
    }

    @Test
    public void shouldNotProcessDataStreamWhenRowtimeIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'rowtime'");

        when(tableSchema.getColumnNames()).thenReturn(new String[]{"longbow_data1", "longbow_key", "longbow_duration", "event_timestamp"});

        LongBowProcessor longBowProcessor = new LongBowProcessor(configuration, table.getSchema().getColumnNames());
        longBowProcessor.validateQuery();
    }

    @Test
    public void shouldNotProcessDataStreamWhenMultipleFieldsAreMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'event_timestamp,rowtime'");

        when(tableSchema.getColumnNames()).thenReturn(new String[]{"longbow_data1", "longbow_key", "longbow_duration"});

        LongBowProcessor longBowProcessor = new LongBowProcessor(configuration, table.getSchema().getColumnNames());
        longBowProcessor.validateQuery();
    }

    @Test
    public void shouldNotThrowExceptionWhenAllFieldsPresentInQuery() {
        when(tableSchema.getColumnNames()).thenReturn(new String[]{"rowtime", "longbow_key", "longbow_duration", "event_timestamp", "longbow_data1"});

        LongBowProcessor longBowProcessor = new LongBowProcessor(configuration, table.getSchema().getColumnNames());
        Assert.assertTrue(longBowProcessor.validateQuery());
    }
}
