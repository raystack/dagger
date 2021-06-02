package io.odpf.dagger.functions.udfs.scalar;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TimeInDateTest {

    private final TimeInDate timeInDate = new TimeInDate();

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private FunctionContext functionContext;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "TimeInDate")).thenReturn(metricGroup);
    }

    @Test
    public void shouldReturnTimestampOfDayAtGivenHourAndMinuteIST() {
        long earliestTimestamp = timeInDate.eval(Long.parseLong("1562224758"), 21, 0, "Asia/Kolkata");
        assertEquals(Long.parseLong("1562254200"), earliestTimestamp);
    }

    @Test
    public void shouldReturnTimestampOfDayAtGivenHourAndMinuteWIB() {
        long earliestTimestamp = timeInDate.eval(Long.parseLong("1562224758"), 21, 0, "Asia/Jakarta");
        assertEquals(Long.parseLong("1562248800"), earliestTimestamp);
    }

    @Test
    public void shouldReturnTimestampOfDayAtGivenHourAndMinuteUTC() {
        long earliestTimestamp = timeInDate.eval(Long.parseLong("1562224758"), 21, 0, "UTC");
        assertEquals(Long.parseLong("1562274000"), earliestTimestamp);
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        timeInDate.open(functionContext);

        Mockito.verify(metricGroup, times(1)).gauge(ArgumentMatchers.any(String.class), ArgumentMatchers.any(Gauge.class));
    }
}
