package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class CounterStatsManagerTest {
    @Mock
    private MetricGroup metricGroup;

    private CounterStatsManager counterStatsManager;

    @Before
    public void setup() {
        initMocks(this);
        counterStatsManager = new CounterStatsManager(metricGroup);
    }

    @Test
    public void shouldRegisterCounterForCounterAspects() {
        when(metricGroup.addGroup("counterTest")).thenReturn(metricGroup);
        counterStatsManager.registerAspects( TestAspects.values(), "counterTest");
        verify(metricGroup, times(1)).counter(any(String.class));
    }

    enum TestAspects implements Aspects {
        TEST_ASPECT_ONE("test_aspect1", AspectType.Histogram),
        TEST_ASPECT_TWO("test_aspect2", AspectType.Metric),
        TEST_ASPECT_THREE("test_aspect3", AspectType.Counter);

        private String value;
        private AspectType aspectType;

        TestAspects(String value, AspectType aspectType) {
            this.value = value;
            this.aspectType = aspectType;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public AspectType getAspectType() {
            return aspectType;
        }
    }
}