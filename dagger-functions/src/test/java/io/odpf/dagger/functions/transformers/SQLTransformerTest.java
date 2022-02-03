package io.odpf.dagger.functions.transformers;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SQLTransformerTest {

    @Mock
    private DataStream<Row> inputStream;

    @Mock
    private StreamExecutionEnvironment streamExecutionEnvironment;

    @Mock
    private StreamTableEnvironment streamTableEnvironment;

    @Mock
    private Table table;

    @Mock
    private TableSchema tableSchema;

    @Mock
    private DataStream<Tuple2<Boolean, Row>> retractStream;

    @Mock
    private SingleOutputStreamOperator<Tuple2<Boolean, Row>> filteredRetractStream;

    @Mock
    private SingleOutputStreamOperator outputStream;

    @Mock
    private SingleOutputStreamOperator watermarkedStream;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
        when(inputStream.getExecutionEnvironment()).thenReturn(streamExecutionEnvironment);
    }

    @Test
    public void shouldApplyThePassedSQL() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        String sqlQuery = "SELECT * FROM data_stream";
        transformationArguments.put("sqlQuery", sqlQuery);
        String[] columnNames = {"order_number", "service_type", "status"};
        String schema = String.join(",", columnNames);

        when(streamTableEnvironment.sqlQuery(sqlQuery)).thenReturn(table);
        when(table.getSchema()).thenReturn(tableSchema);
        when(tableSchema.getFieldNames()).thenReturn(columnNames);
        when(streamTableEnvironment.toRetractStream(table, Row.class)).thenReturn(retractStream);
        when(retractStream.filter(any())).thenReturn(filteredRetractStream);
        when(filteredRetractStream.map(any())).thenReturn(outputStream);
        SQLTransformer sqlTransformer = new SQLTransformerStub(transformationArguments, columnNames);
        StreamInfo inputStreamInfo = new StreamInfo(inputStream, columnNames);
        StreamInfo outputStreamInfo = sqlTransformer.transform(inputStreamInfo);

        verify(streamTableEnvironment, times(1)).registerDataStream("data_stream", inputStream, schema);
        verify(streamTableEnvironment, times(1)).sqlQuery(sqlQuery);
        assertEquals(outputStream, outputStreamInfo.getDataStream());
    }

    @Test
    public void shouldReturnColumnNamesReturnedBySQL() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        String sqlQuery = "SELECT order_number, service_type FROM data_stream";
        transformationArguments.put("sqlQuery", sqlQuery);
        String[] columnNames = {"order_number", "service_type", "status"};

        when(streamTableEnvironment.sqlQuery(sqlQuery)).thenReturn(table);
        when(table.getSchema()).thenReturn(tableSchema);
        String[] outputColumns = {"order_number", "service_type"};
        when(tableSchema.getFieldNames()).thenReturn(outputColumns);
        when(streamTableEnvironment.toRetractStream(table, Row.class)).thenReturn(retractStream);
        when(retractStream.filter(any())).thenReturn(filteredRetractStream);
        when(filteredRetractStream.map(any())).thenReturn(outputStream);
        SQLTransformer sqlTransformer = new SQLTransformerStub(transformationArguments, columnNames);
        StreamInfo inputStreamInfo = new StreamInfo(inputStream, columnNames);
        StreamInfo outputStreamInfo = sqlTransformer.transform(inputStreamInfo);

        assertArrayEquals(outputColumns, outputStreamInfo.getColumnNames());
    }

    @Test
    public void shouldAssignTimestampAndWatermarksIfRowtimeIsPassedInColumns() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        String sqlQuery = "SELECT * FROM data_stream";
        transformationArguments.put("sqlQuery", sqlQuery);
        String[] columnNames = {"order_number", "service_type", "rowtime"};
        String schema = "order_number,service_type,rowtime.rowtime";

        when(streamTableEnvironment.sqlQuery(sqlQuery)).thenReturn(table);
        when(table.getSchema()).thenReturn(tableSchema);
        when(tableSchema.getFieldNames()).thenReturn(columnNames);
        when(streamTableEnvironment.toRetractStream(table, Row.class)).thenReturn(retractStream);
        when(retractStream.filter(any())).thenReturn(filteredRetractStream);
        when(filteredRetractStream.map(any())).thenReturn(outputStream);
        when(inputStream.assignTimestampsAndWatermarks(any(WatermarkStrategy.class))).thenReturn(watermarkedStream);
        SQLTransformer sqlTransformer = new SQLTransformerStub(transformationArguments, columnNames);
        StreamInfo inputStreamInfo = new StreamInfo(inputStream, columnNames);
        StreamInfo outputStreamInfo = sqlTransformer.transform(inputStreamInfo);

        verify(streamTableEnvironment, times(1)).registerDataStream("data_stream", watermarkedStream, schema);
        verify(streamTableEnvironment, times(1)).sqlQuery(sqlQuery);
        verify(inputStream, times(1)).assignTimestampsAndWatermarks(any(WatermarkStrategy.class));
        assertEquals(outputStream, outputStreamInfo.getDataStream());
    }

    @Test
    public void shouldNotAssignTimestampAndWatermarksIfRowtimeIsNotPassedInColumns() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        String sqlQuery = "SELECT * FROM data_stream";
        transformationArguments.put("sqlQuery", sqlQuery);
        String[] columnNames = {"order_number", "service_type", "status"};
        String schema = "order_number,service_type,status";

        when(streamTableEnvironment.sqlQuery(sqlQuery)).thenReturn(table);
        when(table.getSchema()).thenReturn(tableSchema);
        when(tableSchema.getFieldNames()).thenReturn(columnNames);
        when(streamTableEnvironment.toRetractStream(table, Row.class)).thenReturn(retractStream);
        when(retractStream.filter(any())).thenReturn(filteredRetractStream);
        when(filteredRetractStream.map(any())).thenReturn(outputStream);
        SQLTransformer sqlTransformer = new SQLTransformerStub(transformationArguments, columnNames);
        StreamInfo inputStreamInfo = new StreamInfo(inputStream, columnNames);
        StreamInfo outputStreamInfo = sqlTransformer.transform(inputStreamInfo);

        verify(streamTableEnvironment, times(1)).registerDataStream("data_stream", inputStream, schema);
        verify(streamTableEnvironment, times(1)).sqlQuery(sqlQuery);
        verify(inputStream, times(0)).assignTimestampsAndWatermarks(any(BoundedOutOfOrdernessTimestampExtractor.class));
        assertEquals(outputStream, outputStreamInfo.getDataStream());
    }

    @Test
    public void shouldAssignPassedTableNameIfPassedInArguments() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        String sqlQuery = "SELECT * FROM data_stream";
        transformationArguments.put("sqlQuery", sqlQuery);
        transformationArguments.put("tableName", "booking");
        String[] columnNames = {"order_number", "service_type", "status"};
        String schema = "order_number,service_type,status";

        when(streamTableEnvironment.sqlQuery(sqlQuery)).thenReturn(table);
        when(table.getSchema()).thenReturn(tableSchema);
        when(tableSchema.getFieldNames()).thenReturn(columnNames);
        when(streamTableEnvironment.toRetractStream(table, Row.class)).thenReturn(retractStream);
        when(retractStream.filter(any())).thenReturn(filteredRetractStream);
        when(filteredRetractStream.map(any())).thenReturn(outputStream);
        SQLTransformer sqlTransformer = new SQLTransformerStub(transformationArguments, columnNames);
        StreamInfo inputStreamInfo = new StreamInfo(inputStream, columnNames);
        StreamInfo outputStreamInfo = sqlTransformer.transform(inputStreamInfo);

        verify(streamTableEnvironment, times(1)).registerDataStream("booking", inputStream, schema);
        verify(streamTableEnvironment, times(1)).sqlQuery(sqlQuery);
        assertEquals(outputStream, outputStreamInfo.getDataStream());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfSqlNotProvided() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        String[] columnNames = {"order_number", "service_type", "status"};
        SQLTransformer sqlTransformer = new SQLTransformerStub(transformationArguments, columnNames);
        StreamInfo inputStreamInfo = new StreamInfo(inputStream, columnNames);
        sqlTransformer.transform(inputStreamInfo);
    }

    class SQLTransformerStub extends SQLTransformer {

        SQLTransformerStub(Map<String, String> transformationArguments, String[] columnNames) {
            super(transformationArguments, columnNames, configuration);
        }

        @Override
        protected StreamTableEnvironment getStreamTableEnvironment(StreamExecutionEnvironment executionEnvironment) {
            return streamTableEnvironment;
        }
    }

}
