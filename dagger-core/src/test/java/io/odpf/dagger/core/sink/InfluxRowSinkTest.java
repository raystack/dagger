package io.odpf.dagger.core.sink;

import io.odpf.dagger.core.sink.influx.ErrorHandler;
import io.odpf.dagger.core.sink.influx.InfluxDBFactoryWrapper;
import io.odpf.dagger.core.sink.influx.InfluxRowSink;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;

import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.utils.Constants;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class InfluxRowSinkTest {

    private static final int SINK_INFLUX_BATCH_SIZE = 100;
    private static final int INFLUX_FLUSH_DURATION = 1000;
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    private Configuration parameters;
    @Mock
    private InfluxDBFactoryWrapper influxDBFactory;
    @Mock
    private InfluxDB influxDb;
    @Mock
    private InfluxRowSink influxRowSink;
    @Mock
    private RuntimeContext runtimeContext;
    @Mock
    private MetricGroup metricGroup;
    @Mock
    private ErrorReporter errorReporter;
    @Mock
    private Counter counter;
    private ErrorHandler errorHandler = new ErrorHandler();

    @Before
    public void setUp() throws Exception {
        parameters = new Configuration();
        parameters.setString("SINK_INFLUX_URL", "http://localhost:1111");
        parameters.setString("SINK_INFLUX_USERNAME", "usr");
        parameters.setString("SINK_INFLUX_PASSWORD", "pwd");
        parameters.setInteger("SINK_INFLUX_BATCH_SIZE", SINK_INFLUX_BATCH_SIZE);
        parameters.setInteger("SINK_INFLUX_FLUSH_DURATION_MS", INFLUX_FLUSH_DURATION);
        parameters.setString("SINK_INFLUX_DB_NAME", "dagger_test");
        parameters.setString("SINK_INFLUX_RETENTION_POLICY", "two_day_policy");
        parameters.setString("SINK_INFLUX_MEASUREMENT_NAME", "test_table");
        when(influxDBFactory.connect(any(), any(), any())).thenReturn(influxDb);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.SINK_INFLUX_LATE_RECORDS_DROPPED_KEY)).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY,
                InfluxDBException.class.getName())).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
    }

    private void setupInfluxDB(String[] rowColumns) throws Exception {
        influxRowSink = new InfluxRowSink(influxDBFactory, rowColumns, parameters, errorHandler);
        influxRowSink.open(null);
    }

    private void setupStubedInfluxDB(String[] rowColumns) throws Exception {
        influxRowSink = new InfluxRowSinkStub(influxDBFactory, rowColumns, parameters, errorHandler, errorReporter);
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
        setupStubedInfluxDB(new String[]{});

        verify(influxDBFactory).connect("http://localhost:1111", "usr", "pwd");
    }

    @Test
    public void shouldCallBatchModeOnInfluxWhenBatchSettingsExist() throws Exception {
        setupStubedInfluxDB(new String[]{});

        verify(influxDb).enableBatch(eq(SINK_INFLUX_BATCH_SIZE), eq(INFLUX_FLUSH_DURATION), eq(TimeUnit.MILLISECONDS), any(ThreadFactory.class), any(BiConsumer.class));
    }

    @Test
    public void shouldCloseInfluxDBWhenCloseCalled() throws Exception {
        setupStubedInfluxDB(new String[]{});

        influxRowSink.close();

        verify(influxDb).close();
    }

    @Test
    public void shouldWriteToConfiguredInfluxDatabase() throws Exception {
        setupStubedInfluxDB(new String[]{"some_field_name"});

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

        setupStubedInfluxDB(rowColumns);

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

        setupStubedInfluxDB(rowColumns);

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

        setupStubedInfluxDB(rowColumns);

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

        setupStubedInfluxDB(rowColumns);

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

        setupStubedInfluxDB(rowColumns);

        influxRowSink.invoke(simpleFieldsRow, null);

        ArgumentCaptor<Point> pointArg = ArgumentCaptor.forClass(Point.class);
        verify(influxDb).write(any(), any(), pointArg.capture());

        assertEquals(expectedPoint.lineProtocol(), pointArg.getValue().lineProtocol());
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowIfExceptionInWrite() throws Exception {
        setupStubedInfluxDB(new String[]{"some_field_name"});

        Row row = new Row(1);
        row.setField(0, "some field");

        doThrow(new RuntimeException()).when(influxDb).write(any(), any(), any());

        influxRowSink.invoke(row, null);
    }

    @Test
    public void shouldReportIncaseOfFatalError() throws Exception {

        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();
        setupStubedInfluxDB(rowColumns);
        ArrayList points = new ArrayList<Point>();
        points.add(point);
        try {
            errorHandler.getExceptionHandler().accept(points, new RuntimeException("exception from handler"));
            influxRowSink.invoke(getRow(), null);
        } catch (Exception e) {
            Assert.assertEquals("exception from handler", e.getMessage());
        }

        verify(errorReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldReportInCaseOfMaxSeriesExceeded() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();
        setupStubedInfluxDB(rowColumns);
        ArrayList points = new ArrayList<Point>();
        points.add(point);
        try {
            errorHandler.getExceptionHandler().accept(points, new InfluxDBException("{\"error\":\"partial write:"
                    + " max-values-per-tag limit exceeded (100453/100000)"));
            influxRowSink.invoke(getRow(), null);
        } catch (Exception e) {
            Assert.assertEquals("{\"error\":\"partial write: max-values-per-tag limit exceeded (100453/100000)", e.getMessage());
        }

        verify(errorReporter, times(1)).reportFatalException(any(InfluxDBException.class));
    }

    @Test
    public void shouldNotReportInCaseOfFailedRecordFatalError() throws Exception {

        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();
        setupStubedInfluxDB(rowColumns);
        ArrayList points = new ArrayList<Point>();
        points.add(point);
        errorHandler.getExceptionHandler().accept(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));
        influxRowSink.invoke(getRow(), null);

        verify(errorReporter, times(0)).reportFatalException(any());
    }

    @Test
    public void invokeShouldThrowErrorSetByHandler() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();
        setupStubedInfluxDB(rowColumns);
        ArrayList points = new ArrayList<Point>();
        points.add(point);
        errorHandler.getExceptionHandler().accept(points, new RuntimeException("exception from handler"));

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("exception from handler");
        influxRowSink.invoke(getRow(), null);
    }

    @Test
    public void failSnapshotStateOnInfluxError() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        setupStubedInfluxDB(rowColumns);

        errorHandler.getExceptionHandler().accept(new ArrayList<Point>(), new RuntimeException("exception from handler"));

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("exception from handler");
        influxRowSink.snapshotState(null);
    }

    @Test
    public void failSnapshotStateOnFlushFailure() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        setupStubedInfluxDB(rowColumns);

        Mockito.doThrow(new RuntimeException("exception from flush")).when(influxDb).flush();

        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("exception from flush");
        influxRowSink.snapshotState(null);
    }

    public class InfluxRowSinkStub extends InfluxRowSink {
        public InfluxRowSinkStub(InfluxDBFactoryWrapper influxDBFactory, String[] columnNames,
                                 Configuration parameters, ErrorHandler errorHandler, ErrorReporter errorReporter) {
            super(influxDBFactory, columnNames, parameters, errorHandler, errorReporter);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }

    }
}
