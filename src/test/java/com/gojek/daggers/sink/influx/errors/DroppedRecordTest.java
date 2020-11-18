package com.gojek.daggers.sink.influx.errors;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

import com.gojek.daggers.utils.Constants;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DroppedRecordTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private Iterable<Point> points;

    @Mock
    private Counter counter;

    @Before
    public void setUp() {
        initMocks(this);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(Constants.INFLUX_LATE_RECORDS_DROPPED_KEY)).thenReturn(metricGroup);
        when(metricGroup.counter("value")).thenReturn(counter);
    }

    @Test
    public void shouldFilterOnlyLateRecordDrops() {
        DroppedRecord droppedRecord = new DroppedRecord(runtimeContext);
        Assert.assertTrue(droppedRecord
                .filterError(new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}")));
        Assert.assertFalse(droppedRecord
                .filterError(new ArrayIndexOutOfBoundsException(1)));
    }

    @Test
    public void shouldParseNumberOfFailedPoints() {
        DroppedRecord droppedRecord = new DroppedRecord(runtimeContext);
        droppedRecord.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));

        int numFailedRecords = 11;
        verify(counter, times(1)).inc(numFailedRecords);
    }

    @Test
    public void shouldReportCounterWithNum() {
        DroppedRecord droppedRecord = new DroppedRecord(runtimeContext);
        droppedRecord.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));

        verify(counter, times(1)).inc(any(long.class));
    }

    @Test
    public void shouldNotReturnAnyError() {
        DroppedRecord droppedRecord = new DroppedRecord(runtimeContext);
        Assert.assertNull(droppedRecord.getCurrentException());
    }

    @Test
    public void shouldHaveNoError() {
        DroppedRecord droppedRecord = new DroppedRecord(runtimeContext);
        Assert.assertFalse(droppedRecord.hasException());
    }

    @Test
    public void shouldIncreaseTheCountersInCaseOfMultipleErrors() {
        SimpleCounter counter = new SimpleCounter();
        when(metricGroup.counter("value")).thenReturn(counter);
        DroppedRecord droppedRecord = new DroppedRecord(runtimeContext);
        droppedRecord.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=11\"}"));

        droppedRecord.handle(points,
                new InfluxDBException("{\"error\":\"partial write: points beyond retention policy dropped=5\"}"));

        Assert.assertEquals(16, counter.getCount());
    }
}
