package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class UdfMetricsManagerTest {

    @Mock
    private MetricGroup metricGroup;

    private UdfMetricsManager udfMetricsManager;

    @Before
    public void setup() {
        initMocks(this);
        udfMetricsManager = new UdfMetricsManager(metricGroup);
    }

    @Test
    public void shouldRegisterMetrics() {
        when(metricGroup.addGroup("udf", "AppBetaUsers")).thenReturn(metricGroup);

        udfMetricsManager.registerGauge("AppBetaUsers");
        verify(metricGroup, times(1)).gauge(any(String.class), any(Gauge.class));
    }
}
