package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class MeterStatsManagerTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private MetricGroup metricGroup;

    @Mock
    private HashMap histogramMap;

    @Mock
    private HashMap meterMap;

    @Mock
    private Histogram histogram;

    @Mock
    private Meter meter;

    private MeterStatsManager meterStatsManager;


    @Before
    public void setUp() {
        initMocks(this);
        meterStatsManager = new MeterStatsManager(runtimeContext, true);
    }

    @Test
    public void shouldRegisterHistogramAspects() {
        String groupName = "test_groupName";
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(groupName)).thenReturn(metricGroup);

        meterStatsManager.register(groupName, TestAspects.values());
        verify(metricGroup, times(1)).histogram(any(String.class), any(Histogram.class));
    }

    @Test
    public void shouldRegisterMeterAspects() {
        String groupName = "test_groupName";
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(groupName)).thenReturn(metricGroup);

        meterStatsManager.register(groupName, TestAspects.values());
        verify(metricGroup, times(1)).meter(any(String.class), any(DropwizardMeterWrapper.class));
    }

    @Test
    public void shouldUpdateHistogram() {
        meterStatsManager = new MeterStatsManager(runtimeContext, true, histogramMap, meterMap);
        when(histogramMap.get(TestAspects.TEST_ASPECT_ONE)).thenReturn(histogram);

        meterStatsManager.updateHistogram(TestAspects.TEST_ASPECT_ONE, 100);
        verify(histogram, times(1)).update(100);
    }

    @Test
    public void shouldMarkMeterEvents() {
        meterStatsManager = new MeterStatsManager(runtimeContext, true, histogramMap, meterMap);
        when(meterMap.get(TestAspects.TEST_ASPECT_TWO)).thenReturn(meter);

        meterStatsManager.markEvent(TestAspects.TEST_ASPECT_TWO);
        verify(meter, times(1)).markEvent();
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