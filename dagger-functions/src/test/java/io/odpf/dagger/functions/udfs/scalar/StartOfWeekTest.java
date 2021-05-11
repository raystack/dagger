package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class StartOfWeekTest {

    private final StartOfWeek startOfWeek = new StartOfWeek();

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "StartOfWeek")).thenReturn(metricGroup);
    }

    @Test
    public void shouldTimestampAtEarliestGivenDayIST() {
        long earliestTimestamp = startOfWeek.eval(Long.parseLong("1562224758000"), "Asia/Kolkata");
        assertEquals(Long.parseLong("1561919400000"), earliestTimestamp);
    }

    @Test
    public void shouldTimestampAtEarliestGivenDayWIB() {
        long earliestTimestamp = startOfWeek.eval(Long.parseLong("1562224758000"), "Asia/Jakarta");
        assertEquals(Long.parseLong("1561914000000"), earliestTimestamp);
    }

    @Test
    public void shouldTimestampAtEarliestGivenDayUTC() {
        long earliestTimestamp = startOfWeek.eval(Long.parseLong("1562224758000"), "UTC");
        assertEquals(Long.parseLong("1561939200000"), earliestTimestamp);
    }

    @Test
    public void shouldTimestampAtEarliestGivenDayUTCWithEventTimestampIsMondayDayStart() {
        long earliestTimestamp = startOfWeek.eval(Long.parseLong("1561939200000"), "UTC");
        assertEquals(Long.parseLong("1561939200000"), earliestTimestamp);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        startOfWeek.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
