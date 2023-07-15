package org.raystack.dagger.functions.transformers;

import org.raystack.dagger.common.core.DaggerContextTestBase;
import org.raystack.dagger.common.core.DaggerContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import org.raystack.dagger.common.core.StreamInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SQLTransformerTest extends DaggerContextTestBase {

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

    private Multiply multiply;

    @Before
    public void setup() {
        initMocks(this);
        setMock(daggerContext);
        when(daggerContext.getConfiguration()).thenReturn(configuration);
        when(inputStream.getExecutionEnvironment()).thenReturn(streamExecutionEnvironment);
        when(daggerContext.getExecutionEnvironment()).thenReturn(streamExecutionEnvironment);
        when(daggerContext.getTableEnvironment()).thenReturn(streamTableEnvironment);
        multiply = new Multiply();
    }

    @After
    public void resetSingleton() throws Exception {
        Field instance = DaggerContext.class.getDeclaredField("daggerContext");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    private void setMock(DaggerContext mock) {
        try {
            Field instance = DaggerContext.class.getDeclaredField("daggerContext");
            instance.setAccessible(true);
            instance.set(instance, mock);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    public void shouldReturnColumnNamesReturnedBySQLAndUdf() {
        HashMap<String, String> transformationArguments = new HashMap<>();
        String sqlQuery = "SELECT order_number, service_type, multiply(1,100) product FROM data_stream";
        transformationArguments.put("sqlQuery", sqlQuery);
        String[] columnNames = {"order_number", "service_type", "status", "product"};
        streamTableEnvironment.createTemporaryFunction("multiply", multiply);

        when(streamTableEnvironment.sqlQuery(sqlQuery)).thenReturn(table);
        when(table.getSchema()).thenReturn(tableSchema);
        String[] outputColumns = {"order_number", "service_type", "product"};
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
            super(transformationArguments, columnNames, daggerContext);
        }
    }

    class Multiply extends ScalarFunction {
        public Integer eval(Integer i1, Integer i2) {
            return i1 * i2;
        }
    }

}
