package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.DistinctCountAccumulator;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class DistinctCountTest {

    @Mock
    private FunctionContext functionContext;

    @Mock
    private MetricGroup metricGroup;

    @Before
    public void setup() {
        initMocks(this);
        when(functionContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("udf", "DistinctCount")).thenReturn(metricGroup);
    }

    @Test
    public void shouldNotAddItemIfItAlreadyExistsInDistinctItems() {
        DistinctCountAccumulator distinctCountAccumulator = new DistinctCountAccumulator();
        DistinctCount distinctCount = new DistinctCount();
        distinctCount.accumulate(distinctCountAccumulator, "1234");
        distinctCount.accumulate(distinctCountAccumulator, "1234");
        distinctCount.accumulate(distinctCountAccumulator, "1233");
        assertEquals(new Integer(2), distinctCount.getValue(distinctCountAccumulator));
    }

    @Test
    public void shouldNotAddNull() {
        DistinctCountAccumulator distinctCountAccumulator = new DistinctCountAccumulator();
        DistinctCount distinctCount = new DistinctCount();
        distinctCount.accumulate(distinctCountAccumulator, null);
        assertEquals(new Integer(0), distinctCount.getValue(distinctCountAccumulator));
    }

    @Test
    public void shouldNotHoldState() {
        DistinctCount distinctCount = new DistinctCount();
        DistinctCountAccumulator acc1 = distinctCount.createAccumulator();
        DistinctCountAccumulator acc2 = distinctCount.createAccumulator();

        distinctCount.accumulate(acc1, "111");
        distinctCount.accumulate(acc1, "222");
        distinctCount.accumulate(acc1, "222");
        distinctCount.accumulate(acc1, "333");

        distinctCount.accumulate(acc2, "444");
        distinctCount.accumulate(acc2, "555");

        assertEquals(new Integer(3), distinctCount.getValue(acc1));
        assertEquals(new Integer(2), distinctCount.getValue(acc2));
    }

    @Test
    public void shouldRegisterGauge() throws Exception {
        DistinctCount distinctCount = new DistinctCount();
        distinctCount.open(functionContext);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
