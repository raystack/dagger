package io.odpf.dagger.core.sink.influx.errors;

import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import io.odpf.dagger.core.utils.Constants;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LateRecordDropErrorTest {

    @Mock
    private InitContext initContext;

    @Mock
    private SinkWriterMetricGroup metricGroup;

    @Mock
    private MetricGroup metricGroupForLateRecords;

    @Mock
    private Iterable<Point> points;

    @Mock
    private Counter counter;

    @Before
    public void setUp() {
        initMocks(this);
        when(initContext.metricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.SINK_INFLUX_LATE_RECORDS_DROPPED_KEY)).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.NONFATAL_EXCEPTION_METRIC_GROUP_KEY,
                InfluxDBException.class.getName())).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
    }

    @Test
    public void shouldFilterLateRecordDrops() {
        LateRecordDropError lateRecordDropError = new LateRecordDropError(initContext);
        assertTrue(lateRecordDropError
                .filterError(new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}")));
    }

    @Test
    public void shouldNotFilterAnythingElseExceptRecordDrops() {
        LateRecordDropError lateRecordDropError = new LateRecordDropError(initContext);
        assertFalse(lateRecordDropError
                .filterError(new InfluxDBException("{\"error\":\"partial write: max-values-per-tag limit exceeded (100453/100000)")));
    }

    @Test
    public void shouldParseNumberOfFailedPoints() {
        LateRecordDropError lateRecordDropError = new LateRecordDropError(initContext);
        lateRecordDropError.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));

        int numFailedRecords = 11;
        verify(counter, times(1)).inc(numFailedRecords);
    }

    @Test
    public void shouldReportNonFatalExceptionsInHandle() {
        LateRecordDropError lateRecordDropError = new LateRecordDropError(initContext);
        lateRecordDropError.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));

        verify(counter, times(1)).inc();
    }

    @Test
    public void shouldReportCounterWithNum() {
        LateRecordDropError lateRecordDropError = new LateRecordDropError(initContext);
        lateRecordDropError.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));

        verify(counter, times(1)).inc(any(long.class));
    }

    @Test
    public void shouldNotReturnAnyError() {
        LateRecordDropError lateRecordDropError = new LateRecordDropError(initContext);
        assertNull(lateRecordDropError.getCurrentException());
    }

    @Test
    public void shouldHaveNoError() {
        LateRecordDropError lateRecordDropError = new LateRecordDropError(initContext);
        assertFalse(lateRecordDropError.hasException());
    }

    @Test
    public void shouldIncreaseTheCountersInCaseOfMultipleErrors() {
        SimpleCounter simpleCounter = new SimpleCounter();
        when(metricGroup.addGroup(Constants.SINK_INFLUX_LATE_RECORDS_DROPPED_KEY)).thenReturn(metricGroupForLateRecords);
        when(metricGroupForLateRecords.counter("value")).thenReturn(simpleCounter);
        LateRecordDropError lateRecordDropError = new LateRecordDropError(initContext);
        lateRecordDropError.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));

        lateRecordDropError.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=5\"}"));

        assertEquals(16, simpleCounter.getCount());
    }
}
