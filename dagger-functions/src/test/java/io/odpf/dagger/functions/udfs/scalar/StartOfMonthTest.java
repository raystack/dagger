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

public class StartOfMonthTest {

    private StartOfMonth startOfMonth = new StartOfMonth();
    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "StartOfMonth")).thenReturn(metricGroup);
    }

    @Test
    public void shouldGiveTimestampOfFirstDateOfTheMonthForAGivenDateInTimezoneIST() {
        long startOfMonthTimestamp = startOfMonth.eval(Long.parseLong("1562224758"), "Asia/Kolkata");
        assertEquals(Long.parseLong("1561919400"), startOfMonthTimestamp);
    }

    @Test
    public void shouldGiveTimestampOfFirstDateOfTheMonthForAGivenDateInTimezoneUTC() {
        long startOfMonthTimestamp = startOfMonth.eval(Long.parseLong("1562224758"), "UTC");
        assertEquals(Long.parseLong("1561939200"), startOfMonthTimestamp);
    }

    @Test
    public void shouldGiveTimestampOfFirstDateOfTheMonthForAGivenDateInTimezoneWIB() {
        long startOfMonthTimestamp = startOfMonth.eval(Long.parseLong("1562224758"), "Asia/Jakarta");
        assertEquals(Long.parseLong("1561914000"), startOfMonthTimestamp);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        startOfMonth.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
