package io.odpf.dagger.common.metrics.managers;

import io.odpf.dagger.common.metrics.managers.utils.TestAspects;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
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
    public void shouldRegisterIntegerGaugeForAllTheAspects() {
        when(metricGroup.addGroup("test_key", "test_value")).thenReturn(metricGroup);
        gaugeStatsManager.registerAspects("test_key", "test_value", TestAspects.values(), 1);
        verify(metricGroup, times(3)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldRegisterIntegerGaugeForSingleAspect() {
        when(metricGroup.addGroup("test_key", "test_value")).thenReturn(metricGroup);
        gaugeStatsManager.registerInteger("test_key", "test_value", TestAspects.TEST_ASPECT_ONE.getValue(), 1);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldRegisterStringGaugeForSingleAspect() {
        when(metricGroup.addGroup("test_key", "test_value")).thenReturn(metricGroup);
        gaugeStatsManager.registerString("test_key", "test_value", TestAspects.TEST_ASPECT_ONE.getValue(), "value");
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldRegisterDoubleGaugeForSingleAspect() {
        when(metricGroup.addGroup("test_key", "test_value")).thenReturn(metricGroup);
        gaugeStatsManager.registerDouble("test_key", "test_value", TestAspects.TEST_ASPECT_ONE.getValue(), 0.01D);
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
