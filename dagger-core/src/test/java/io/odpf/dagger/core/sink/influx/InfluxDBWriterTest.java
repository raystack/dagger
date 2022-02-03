package io.odpf.dagger.core.sink.influx;

import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.api.connector.sink.SinkWriter.Context;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.exception.InfluxWriteException;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static io.odpf.dagger.core.utils.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class InfluxDBWriterTest {

    @Mock
    private Configuration configuration;

    @Mock
    private InfluxDBFactoryWrapper influxDBFactory;

    @Mock
    private InfluxDB influxDb;

    @Mock
    private SinkWriterMetricGroup metricGroup;

    @Mock
    private ErrorReporter errorReporter;

    @Mock
    private Counter counter;

    @Mock
    private Context context;

    @Mock
    private InitContext initContext;

    private ErrorHandler errorHandler = new ErrorHandler();

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        when(configuration.getString(SINK_INFLUX_DB_NAME_KEY, SINK_INFLUX_DB_NAME_DEFAULT)).thenReturn("dagger_test");
        when(configuration.getString(SINK_INFLUX_RETENTION_POLICY_KEY, SINK_INFLUX_RETENTION_POLICY_DEFAULT)).thenReturn("two_day_policy");
        when(configuration.getString(SINK_INFLUX_MEASUREMENT_NAME_KEY, SINK_INFLUX_MEASUREMENT_NAME_DEFAULT)).thenReturn("test_table");
        when(initContext.metricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.SINK_INFLUX_LATE_RECORDS_DROPPED_KEY)).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY,
                InfluxDBException.class.getName())).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
    }

    private Row getRow() {
        final int numberOfRows = 3;
        final int integerTag = 123;
        final int expectedFieldOneValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, integerTag);
        simpleFieldsRow.setField(1, expectedFieldOneValue);
        simpleFieldsRow.setField(2, LocalDateTime.ofInstant(now, ZoneOffset.UTC));
        return simpleFieldsRow;
    }

    private Point getPoint() {
        final int numberOfRows = 3;
        final int integerTag = 123;
        final int expectedFieldOneValue = 100;
        Instant now = Instant.now();
        Row simpleFieldsRow = new Row(numberOfRows);
        simpleFieldsRow.setField(0, integerTag);
        simpleFieldsRow.setField(1, expectedFieldOneValue);
        simpleFieldsRow.setField(2, LocalDateTime.ofInstant(now, ZoneOffset.UTC));
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        return Point.measurement("test_table")
                .tag(rowColumns[0], String.valueOf(integerTag))
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(now.toEpochMilli(), TimeUnit.MILLISECONDS).build();
    }

    @Test
    public void shouldWriteToConfiguredInfluxDatabase() throws Exception {
        Row row = new Row(1);
        row.setField(0, "some field");
        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, new String[]{"some_field_name"}, errorHandler, errorReporter);
        influxDBWriter.write(row, context);

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
        simpleFieldsRow.setField(2, LocalDateTime.ofInstant(now, ZoneOffset.UTC));
        String[] rowColumns = {"field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .addField(rowColumns[0], expectedFieldZeroValue)
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(now.toEpochMilli(), TimeUnit.MILLISECONDS).build();

        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        influxDBWriter.write(simpleFieldsRow, context);
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
        simpleFieldsRow.setField(2, LocalDateTime.ofInstant(now, ZoneOffset.UTC));
        String[] rowColumns = {"field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .addField(rowColumns[0], integerValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        influxDBWriter.write(simpleFieldsRow, context);
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
        simpleFieldsRow.setField(2, LocalDateTime.ofInstant(now, ZoneOffset.UTC));
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .tag(rowColumns[0], expectedFieldZeroValue)
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        influxDBWriter.write(simpleFieldsRow, context);
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
        simpleFieldsRow.setField(2, LocalDateTime.ofInstant(now, ZoneOffset.UTC));
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .tag(rowColumns[0], String.valueOf(integerTag))
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        influxDBWriter.write(simpleFieldsRow, context);
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
        simpleFieldsRow.setField(2, LocalDateTime.ofInstant(now, ZoneOffset.UTC));
        String[] rowColumns = {"label_field1", "field2", "window_timestamp"};
        Point expectedPoint = Point.measurement("test_table")
                .tag(rowColumns[0].substring("label_".length()), expectedFieldZeroValue)
                .addField(rowColumns[1], expectedFieldOneValue)
                .time(Timestamp.from(now).getTime(), TimeUnit.MILLISECONDS).build();

        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        influxDBWriter.write(simpleFieldsRow, context);
        ArgumentCaptor<Point> pointArg = ArgumentCaptor.forClass(Point.class);
        verify(influxDb).write(any(), any(), pointArg.capture());

        assertEquals(expectedPoint.lineProtocol(), pointArg.getValue().lineProtocol());
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowIfExceptionInWrite() throws Exception {
        String[] columns = {"some_field_name"};
        Row row = new Row(1);
        row.setField(0, "some field");

        doThrow(new RuntimeException()).when(influxDb).write(any(), any(), any());

        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, columns, errorHandler, errorReporter);
        influxDBWriter.write(row, context);
    }

    @Test
    public void shouldReportIncaseOfFatalError() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();
        ArrayList points = new ArrayList<Point>();
        points.add(point);

        errorHandler.init(initContext);

        errorHandler.getExceptionHandler().accept(points, new RuntimeException("exception from handler"));
        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        Exception exception = assertThrows(Exception.class,
                () -> influxDBWriter.write(getRow(), context));
        assertEquals("java.lang.RuntimeException: exception from handler", exception.getMessage());
        verify(errorReporter, times(1)).reportFatalException(any(InfluxWriteException.class));
    }

    @Test
    public void shouldReportInCaseOfMaxSeriesExceeded() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();

        ArrayList points = new ArrayList<Point>();
        points.add(point);

        errorHandler.init(initContext);

        errorHandler.getExceptionHandler().accept(points, new InfluxDBException("{\"error\":\"partial write:"
                + " max-values-per-tag limit exceeded (100453/100000)"));
        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        Exception exception = assertThrows(Exception.class,
                () -> influxDBWriter.write(getRow(), context));
        assertEquals("org.influxdb.InfluxDBException: {\"error\":\"partial write: max-values-per-tag limit exceeded (100453/100000)", exception.getMessage());
        verify(errorReporter, times(1)).reportFatalException(any(InfluxWriteException.class));
    }

    @Test
    public void shouldNotReportInCaseOfFailedRecordFatalError() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();

        ArrayList points = new ArrayList<Point>();
        points.add(point);

        errorHandler.init(initContext);

        errorHandler.getExceptionHandler().accept(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));
        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        influxDBWriter.write(getRow(), context);
        verify(errorReporter, times(0)).reportFatalException(any(InfluxWriteException.class));
    }

    @Test
    public void invokeShouldThrowErrorSetByHandler() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        Point point = getPoint();

        ArrayList points = new ArrayList<Point>();
        points.add(point);

        errorHandler.init(initContext);

        errorHandler.getExceptionHandler().accept(points, new RuntimeException("exception from handler"));
        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);

        InfluxWriteException exception = assertThrows(InfluxWriteException.class,
                () -> influxDBWriter.write(getRow(), context));
        assertEquals("java.lang.RuntimeException: exception from handler", exception.getMessage());
    }

    @Test
    public void failSnapshotStateOnInfluxError() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);

        errorHandler.init(initContext);
        errorHandler.getExceptionHandler().accept(new ArrayList<Point>(), new RuntimeException("exception from handler"));

        InfluxWriteException exception = assertThrows(InfluxWriteException.class,
                () -> influxDBWriter.snapshotState(10L));
        assertEquals("java.lang.RuntimeException: exception from handler", exception.getMessage());
    }

    @Test
    public void failSnapshotStateOnFlushFailure() throws Exception {
        String[] rowColumns = {"tag_field1", "field2", "window_timestamp"};
        InfluxDBWriter influxDBWriter = new InfluxDBWriter(configuration, influxDb, rowColumns, errorHandler, errorReporter);
        Mockito.doThrow(new RuntimeException("exception from flush")).when(influxDb).flush();

        Exception exception = assertThrows(Exception.class,
                () -> influxDBWriter.snapshotState(10L));
        assertEquals("exception from flush", exception.getMessage());
    }
}
