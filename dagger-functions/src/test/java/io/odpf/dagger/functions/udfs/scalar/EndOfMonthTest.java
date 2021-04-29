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

public class EndOfMonthTest {

    private EndOfMonth endOfMonth = new EndOfMonth();

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "EndOfMonth")).thenReturn(metricGroup);
    }

    @Test
    public void shouldReturnLastDateOfMonthForGivenTimestampInIST() {
        long startOfMonthTimestamp = endOfMonth.eval(Long.parseLong("1562224758"), "Asia/Kolkata");
        assertEquals(Long.parseLong("1564597799"), startOfMonthTimestamp);
    }

    @Test
    public void shouldReturnLastDateOfMonthForGivenTimestampInUTC() {
        long startOfMonthTimestamp = endOfMonth.eval(Long.parseLong("1562224758"), "UTC");
        assertEquals(Long.parseLong("1564617599"), startOfMonthTimestamp);
    }

    @Test
    public void shouldReturnLastDateOfMonthForGivenTimestampInWIB() {
        long startOfMonthTimestamp = endOfMonth.eval(Long.parseLong("1562224758"), "Asia/Jakarta");
        assertEquals(Long.parseLong("1564592399"), startOfMonthTimestamp);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        endOfMonth.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
