package io.odpf.dagger.core.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.sink.influx.ErrorHandler;
import io.odpf.dagger.core.sink.influx.InfluxDBFactoryWrapper;
import io.odpf.dagger.core.sink.influx.InfluxRowSink;
import io.odpf.dagger.core.utils.Constants;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_BATCH_SIZE_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_BATCH_SIZE_KEY;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_DB_NAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_DB_NAME_KEY;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_FLUSH_DURATION_MS_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_FLUSH_DURATION_MS_KEY;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_MEASUREMENT_NAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_MEASUREMENT_NAME_KEY;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_PASSWORD_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_PASSWORD_KEY;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_RETENTION_POLICY_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_RETENTION_POLICY_KEY;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_URL_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_URL_KEY;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_USERNAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.SINK_INFLUX_USERNAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class InfluxRowSinkTest {

    private static final int SINK_INFLUX_BATCH_SIZE = 100;
    private static final int INFLUX_FLUSH_DURATION = 1000;

    @Mock
    private Configuration configuration;
    @Mock
    private InfluxDBFactoryWrapper influxDBFactory;
    @Mock
    private InfluxDB influxDb;
    @Mock
    private InfluxRowSink influxRowSink;
    @Mock
    private RuntimeContext runtimeContext;
    @Mock
    private OperatorMetricGroup metricGroup;
    @Mock
    private ErrorReporter errorReporter;
    @Mock
    private Counter counter;
    private ErrorHandler errorHandler = new ErrorHandler();


    @Before
    public void setUp() throws Exception {
        initMocks(this);
        when(configuration.getString(SINK_INFLUX_URL_KEY, SINK_INFLUX_URL_DEFAULT)).thenReturn("http://localhost:1111");
        when(configuration.getString(SINK_INFLUX_USERNAME_KEY, SINK_INFLUX_USERNAME_DEFAULT)).thenReturn("usr");
        when(configuration.getString(SINK_INFLUX_PASSWORD_KEY, SINK_INFLUX_PASSWORD_DEFAULT)).thenReturn("pwd");
        when(configuration.getInteger(SINK_INFLUX_BATCH_SIZE_KEY, SINK_INFLUX_BATCH_SIZE_DEFAULT)).thenReturn(100);

        when(configuration.getInteger(SINK_INFLUX_BATCH_SIZE_KEY, SINK_INFLUX_BATCH_SIZE_DEFAULT)).thenReturn(100);
        when(configuration.getInteger(SINK_INFLUX_FLUSH_DURATION_MS_KEY, SINK_INFLUX_FLUSH_DURATION_MS_DEFAULT)).thenReturn(1000);

        when(configuration.getString(SINK_INFLUX_DB_NAME_KEY, SINK_INFLUX_DB_NAME_DEFAULT)).thenReturn("dagger_test");
        when(configuration.getString(SINK_INFLUX_RETENTION_POLICY_KEY, SINK_INFLUX_RETENTION_POLICY_DEFAULT)).thenReturn("two_day_policy");
        when(configuration.getString(SINK_INFLUX_MEASUREMENT_NAME_KEY, SINK_INFLUX_MEASUREMENT_NAME_DEFAULT)).thenReturn("test_table");
        when(influxDBFactory.connect(any(), any(), any())).thenReturn(influxDb);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.SINK_INFLUX_LATE_RECORDS_DROPPED_KEY)).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY,
                InfluxDBException.class.getName())).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
    }

    private void setupStubedInfluxDB(String[] rowColumns) throws Exception {
        influxRowSink = new InfluxRowSinkStub(influxDBFactory, rowColumns, configuration, errorHandler, errorReporter);
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

        verify(influxDb).enableBatch(eq(Integer.valueOf(SINK_INFLUX_BATCH_SIZE)), eq(Integer.valueOf(INFLUX_FLUSH_DURATION)), eq(TimeUnit.MILLISECONDS), any(ThreadFactory.class), any(BiConsumer.class));
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
        errorHandler.getExceptionHandler().accept(points, new RuntimeException("exception from handler"));
        Exception exception = assertThrows(Exception.class,
                () -> influxRowSink.invoke(getRow(), null));
        assertEquals("exception from handler", exception.getMessage());
        verify(errorReporter, times(1)).reportFatalException(any(RuntimeException.class));
    }

    @Test
    public void shouldReportInCaseOfMaxSeriesExceeded() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();
        setupStubedInfluxDB(rowColumns);
        ArrayList points = new ArrayList<Point>();
        points.add(point);
        errorHandler.getExceptionHandler().accept(points, new InfluxDBException("{\"error\":\"partial write:"
                + " max-values-per-tag limit exceeded (100453/100000)"));
        Exception exception = assertThrows(Exception.class,
                () -> influxRowSink.invoke(getRow(), null));
        assertEquals("{\"error\":\"partial write: max-values-per-tag limit exceeded (100453/100000)", exception.getMessage());
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

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> influxRowSink.invoke(getRow(), null));
        assertEquals("exception from handler", exception.getMessage());
    }

    @Test
    public void failSnapshotStateOnInfluxError() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        setupStubedInfluxDB(rowColumns);

        errorHandler.getExceptionHandler().accept(new ArrayList<Point>(), new RuntimeException("exception from handler"));

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> influxRowSink.snapshotState(null));
        assertEquals("exception from handler", exception.getMessage());
    }

    @Test
    public void failSnapshotStateOnFlushFailure() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        setupStubedInfluxDB(rowColumns);

        Mockito.doThrow(new RuntimeException("exception from flush")).when(influxDb).flush();

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> influxRowSink.snapshotState(null));
        assertEquals("exception from flush", exception.getMessage());
    }

    public class InfluxRowSinkStub extends InfluxRowSink {
        public InfluxRowSinkStub(InfluxDBFactoryWrapper influxDBFactory, String[] columnNames,
                                 Configuration configuration, ErrorHandler errorHandler, ErrorReporter errorReporter) {
            super(influxDBFactory, columnNames, configuration, errorHandler, errorReporter);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }

    }
}
