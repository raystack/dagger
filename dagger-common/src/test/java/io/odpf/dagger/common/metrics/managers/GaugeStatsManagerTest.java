package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


public class GaugeStatsManagerTest {
    @Mock
    private MetricGroup metricGroup;

    private GaugeStatsManager gaugeStatsManager;

    @Before
    public void setup() {
        initMocks(this);
        gaugeStatsManager = new GaugeStatsManager(metricGroup, true);
    }

    @Test
    public void shouldRegisterGaugeForAllTheAspects() {
        when(metricGroup.addGroup("test_key", "test_value")).thenReturn(metricGroup);
        gaugeStatsManager.registerAspects("test_key", "test_value", TestAspects.values(), 1);
        verify(metricGroup, times(2)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldRegisterGaugeForSingleAspect() {
        when(metricGroup.addGroup("test_key", "test_value")).thenReturn(metricGroup);
        gaugeStatsManager.register("test_key", "test_value", TestAspects.TEST_ASPECT_ONE.getValue(), 1);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    enum TestAspects implements Aspects {
        TEST_ASPECT_ONE("test_aspect1", AspectType.Histogram),
        TEST_ASPECT_TWO("test_aspect2", AspectType.Metric);

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