package com.gojek.daggers.sink;

import com.gojek.daggers.sink.influx.InfluxDBFactoryWrapper;
import com.gojek.daggers.sink.influx.InfluxErrorHandler;
import com.gojek.daggers.sink.influx.InfluxRowSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class InfluxRowSinkTest {

    private static final int INFLUX_BATCH_SIZE = 100;
    private static final int INFLUX_FLUSH_DURATION = 1000;
    private Configuration parameters;
    @Mock
    private InfluxDBFactoryWrapper influxDBFactory;
    @Mock
    private InfluxDB influxDb;
    @Mock
    private InfluxRowSink influxRowSink;

    private InfluxErrorHandler influxErrorHandler = new InfluxErrorHandler();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        parameters = new Configuration();
        parameters.setString("INFLUX_URL", "http://localhost:1111");
        parameters.setString("INFLUX_USERNAME", "usr");
        parameters.setString("INFLUX_PASSWORD", "pwd");
        parameters.setInteger("INFLUX_BATCH_SIZE", INFLUX_BATCH_SIZE);
        parameters.setInteger("INFLUX_FLUSH_DURATION_IN_MILLISECONDS", INFLUX_FLUSH_DURATION);
        parameters.setString("INFLUX_DATABASE", "dagger_test");
        parameters.setString("INFLUX_RETENTION_POLICY", "two_day_policy");
        parameters.setString("INFLUX_MEASUREMENT_NAME", "test_table");
        when(influxDBFactory.connect(any(), any(), any())).thenReturn(influxDb);

    }

    private void setupInfluxDB(String[] rowColumns) throws Exception {
        influxRowSink = new InfluxRowSink(influxDBFactory, rowColumns, parameters, influxErrorHandler);
        influxRowSink.open(null);
    }

    private Point getPoint() {
        final int numberOfRows = 3;
        final int integerTag = 123;
        final int expectedFieldOneValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, integerTag);
        simpleFieldsRow.setField(1, expectedFieldOneValue);
        simpleFieldsRow.setField(2, Timestamp.from(now));
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        return Point.measurement("test_table")
                .tag(rowColumns[0], String.valueOf(integerTag))
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();
    }

    private Row getRow() {
        final int numberOfRows = 3;
        final int integerTag = 123;
        final int expectedFieldOneValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, integerTag);
        simpleFieldsRow.setField(1, expectedFieldOneValue);
        simpleFieldsRow.setField(2, Timestamp.from(now));
        return simpleFieldsRow;
    }

    @Test
    public void shouldCallInfluxDbFactoryOnOpen() throws Exception {
        setupInfluxDB(new String[]{});

        verify(influxDBFactory).connect("http://localhost:1111", "usr", "pwd");
    }

    @Test
    public void shouldCallBatchModeOnInfluxWhenBatchSettingsExist() throws Exception {
        setupInfluxDB(new String[]{});

        verify(influxDb).enableBatch(eq(INFLUX_BATCH_SIZE), eq(INFLUX_FLUSH_DURATION), eq(TimeUnit.MILLISECONDS), any(ThreadFactory.class), any(BiConsumer.class));
    }

    @Test
    public void shouldCloseInfluxDBWhenCloseCalled() throws Exception {
        setupInfluxDB(new String[]{});

        influxRowSink.close();

        verify(influxDb).close();
    }

    @Test
    public void shouldWriteToConfiguredInfluxDatabase() throws Exception {
        setupInfluxDB(new String[]{"some_field_name"});

        Row row = new Row(1);
        row.setField(0, "some field");
        influxRowSink.invoke(row, null);

        verify(influxDb).write(eq("dagger_test"), eq("two_day_policy"), any());
    }

    @Test
    public void shouldWriteRowToInfluxAsfields() throws Exception {
        final int numberOfRows = 3;
        final String expectedFieldZeroValue = "abc";
        final int expectedFieldOneValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, expectedFieldZeroValue);
        simpleFieldsRow.setField(1, expectedFieldOneValue);
        simpleFieldsRow.setField(2, Timestamp.from(now));
        String[] rowColumns = {"field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .addField(rowColumns[0], expectedFieldZeroValue)
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        setupInfluxDB(rowColumns);

        influxRowSink.invoke(simpleFieldsRow, null);

        ArgumentCaptor<Point> pointArg = ArgumentCaptor.forClass(Point.class);
        verify(influxDb).write(any(), any(), pointArg.capture());

        assertEquals(expectedPoint.lineProtocol(), pointArg.getValue().lineProtocol());
    }

    @Test
    public void shouldNotWriteNullColumnsInRowToInfluxAsfields() throws Exception {
        final int numberOfRows = 3;
        final String nullValue = null;
        final int integerValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, integerValue);
        simpleFieldsRow.setField(1, nullValue);
        simpleFieldsRow.setField(2, Timestamp.from(now));
        String[] rowColumns = {"field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .addField(rowColumns[0], integerValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        setupInfluxDB(rowColumns);

        influxRowSink.invoke(simpleFieldsRow, null);

        ArgumentCaptor<Point> pointArg = ArgumentCaptor.forClass(Point.class);
        verify(influxDb).write(any(), any(), pointArg.capture());

        assertEquals(expectedPoint.lineProtocol(), pointArg.getValue().lineProtocol());
    }

    @Test
    public void shouldWriteRowWithTagColumns() throws Exception {
        final int numberOfRows = 3;
        final String expectedFieldZeroValue = "abc";
        final int expectedFieldOneValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, expectedFieldZeroValue);
        simpleFieldsRow.setField(1, expectedFieldOneValue);
        simpleFieldsRow.setField(2, Timestamp.from(now));
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .tag(rowColumns[0], expectedFieldZeroValue)
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        setupInfluxDB(rowColumns);

        influxRowSink.invoke(simpleFieldsRow, null);

        ArgumentCaptor<Point> pointArg = ArgumentCaptor.forClass(Point.class);
        verify(influxDb).write(any(), any(), pointArg.capture());

        assertEquals(expectedPoint.lineProtocol(), pointArg.getValue().lineProtocol());
    }

    @Test
    public void shouldWriteRowWithTagColumnsOfTypeInteger() throws Exception {
        final int numberOfRows = 3;
        final int integerTag = 123;
        final int expectedFieldOneValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, integerTag);
        simpleFieldsRow.setField(1, expectedFieldOneValue);
        simpleFieldsRow.setField(2, Timestamp.from(now));
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .tag(rowColumns[0], String.valueOf(integerTag))
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        setupInfluxDB(rowColumns);

        influxRowSink.invoke(simpleFieldsRow, null);

        ArgumentCaptor<Point> pointArg = ArgumentCaptor.forClass(Point.class);
        verify(influxDb).write(any(), any(), pointArg.capture());

        assertEquals(expectedPoint.lineProtocol(), pointArg.getValue().lineProtocol());
    }

    @Test
    public void shouldWriteRowWithLabelColumns() throws Exception {
        final int numberOfRows = 3;
        final String expectedFieldZeroValue = "abc";
        final int expectedFieldOneValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, expectedFieldZeroValue);
        simpleFieldsRow.setField(1, expectedFieldOneValue);
        simpleFieldsRow.setField(2, Timestamp.from(now));
        String[] rowColumns = {"label_field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .tag(rowColumns[0].substring("label_".length()), expectedFieldZeroValue)
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        setupInfluxDB(rowColumns);

        influxRowSink.invoke(simpleFieldsRow, null);

        ArgumentCaptor<Point> pointArg = ArgumentCaptor.forClass(Point.class);
        verify(influxDb).write(any(), any(), pointArg.capture());

        assertEquals(expectedPoint.lineProtocol(), pointArg.getValue().lineProtocol());
    }

    @Test
    public void invokeShouldThrowErrorSetByHandler() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();
        setupInfluxDB(rowColumns);
        ArrayList points = new ArrayList<Point>();
        points.add(point);
        influxErrorHandler.getExceptionHandler().accept(points, new RuntimeException("exception from handler"));

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("exception from handler");
        influxRowSink.invoke(getRow(), null);
    }

    @Test
    public void failSnapshotStateOnInfluxError() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        setupInfluxDB(rowColumns);

        influxErrorHandler.getExceptionHandler().accept(new ArrayList<Point>(), new RuntimeException("exception from handler"));

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("exception from handler");
        influxRowSink.snapshotState(null);
    }

    @Test
    public void failSnapshotStateOnFlushFailure() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        setupInfluxDB(rowColumns);

        Mockito.doThrow(new RuntimeException("exception from flush")).when(influxDb).flush();

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("exception from flush");
        influxRowSink.snapshotState(null);
    }
}

