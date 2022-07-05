package io.odpf.dagger.common.metrics.managers;

import io.odpf.dagger.common.metrics.managers.utils.TestAspects;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class CounterStatsManagerTest {
    @Mock
    private MetricGroup metricGroup;

    @Mock
    private Counter counter;

    private CounterStatsManager counterStatsManager;

    @Before
    public void setup() {
        initMocks(this);
        counterStatsManager = new CounterStatsManager(metricGroup);
    }

    @Test
    public void shouldRegisterCounterForCounterAspects() {
        when(metricGroup.addGroup("counterTest")).thenReturn(metricGroup);
        counterStatsManager.registerAspects(TestAspects.values(), "counterTest");
        verify(metricGroup, times(1)).counter(any(String.class));
    }

    @Test
    public void shouldRegisterCounterForCounterAspectWithGroup() {
        when(metricGroup.addGroup("counterTest")).thenReturn(metricGroup);
        counterStatsManager.register(TestAspects.TEST_ASPECT_THREE, "counterTest");
        verify(metricGroup, times(1)).counter(any(String.class));
    }

    @Test
    public void shouldRegisterCounterForCounterAspectWithGroupKeyAndValue() {
        when(metricGroup.addGroup("counterTestKey", "counterTestValue")).thenReturn(metricGroup);
        counterStatsManager.register(TestAspects.TEST_ASPECT_THREE, "counterTestKey", "counterTestValue");
        verify(metricGroup, times(1)).counter(any(String.class));
    }

    @Test
    public void shouldIncreamentCount() {
        when(metricGroup.addGroup("counterTest")).thenReturn(metricGroup);
        when(metricGroup.counter("test_aspect3")).thenReturn(counter);
        counterStatsManager.registerAspects(TestAspects.values(), "counterTest");
        counterStatsManager.inc(TestAspects.TEST_ASPECT_THREE);
        counterStatsManager.inc(TestAspects.TEST_ASPECT_THREE);
        verify(counter, times(2)).inc();
    }

    @Test
    public void shouldUpdateCountAndReturnTheCorrectCountValue() {
        Counter simpleCounter = new SimpleCounter();
        when(metricGroup.addGroup("counterTest")).thenReturn(metricGroup);
        when(metricGroup.counter("test_aspect3")).thenReturn(simpleCounter);
        counterStatsManager.registerAspects(TestAspects.values(), "counterTest");
        counterStatsManager.inc(TestAspects.TEST_ASPECT_THREE);
        counterStatsManager.inc(TestAspects.TEST_ASPECT_THREE);
        long count = counterStatsManager.getCount(TestAspects.TEST_ASPECT_THREE);
        assertEquals(2, count);
    }

}
