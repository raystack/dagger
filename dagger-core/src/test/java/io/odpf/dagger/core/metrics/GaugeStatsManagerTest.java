package io.odpf.dagger.core.metrics;

import io.odpf.dagger.core.metrics.aspects.TelemetryAspects;
import io.odpf.dagger.core.metrics.aspects.LongbowWriterAspects;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class GaugeStatsManagerTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private MetricGroup metricGroup;

    private GaugeStatsManager gaugeStatsManager;

    @Before
    public void setup() {
        initMocks(this);
        gaugeStatsManager = new GaugeStatsManager(runtimeContext, true);
    }

    @Test
    public void shouldRegisterGaugeOnMetricGroup() {
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("test_key", "test_value")).thenReturn(metricGroup);
        gaugeStatsManager.register("test_key", "test_value", TelemetryAspects.values(), 1);

        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }

    @Test
    public void shouldRegisterGaugeForAllTheAspects() {
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup("test_key", "test_value")).thenReturn(metricGroup);

        gaugeStatsManager.register("test_key", "test_value", LongbowWriterAspects.values(), 1);
        verify(metricGroup, times(10)).gauge(any(String.class), any(Gauge.class));
    }

}