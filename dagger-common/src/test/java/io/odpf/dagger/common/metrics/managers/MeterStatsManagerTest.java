package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.common.metrics.managers.utils.TestAspects;
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
        meterStatsManager = new MeterStatsManager(metricGroup, true);
    }

    @Test
    public void shouldRegisterHistogramAspects() {
        String groupName = "test_groupName";
        when(metricGroup.addGroup(groupName)).thenReturn(metricGroup);

        meterStatsManager.register(groupName, TestAspects.values());
        verify(metricGroup, times(1)).histogram(any(String.class), any(Histogram.class));
    }

    @Test
    public void shouldRegisterMeterAspects() {
        String groupName = "test_groupName";
        when(metricGroup.addGroup(groupName)).thenReturn(metricGroup);

        meterStatsManager.register(groupName, TestAspects.values());
        verify(metricGroup, times(1)).meter(any(String.class), any(DropwizardMeterWrapper.class));
    }

    @Test
    public void shouldRegisterMeterAspectsWithGroupKeyAndGroupValue() {
        String groupKey = "test_groupKey";
        String groupValue = "test_groupValue";
        when(metricGroup.addGroup(groupKey, groupValue)).thenReturn(metricGroup);

        meterStatsManager.register(groupKey, groupValue, TestAspects.values());
        verify(metricGroup, times(1)).meter(any(String.class), any(DropwizardMeterWrapper.class));
    }

    @Test
    public void shouldUpdateHistogram() {
        meterStatsManager = new MeterStatsManager(metricGroup, true, histogramMap, meterMap);
        when(histogramMap.get(TestAspects.TEST_ASPECT_ONE)).thenReturn(histogram);

        meterStatsManager.updateHistogram(TestAspects.TEST_ASPECT_ONE, 100);
        verify(histogram, times(1)).update(100);
    }

    @Test
    public void shouldMarkMeterEvents() {
        meterStatsManager = new MeterStatsManager(metricGroup, true, histogramMap, meterMap);
        when(meterMap.get(TestAspects.TEST_ASPECT_TWO)).thenReturn(meter);

        meterStatsManager.markEvent(TestAspects.TEST_ASPECT_TWO);
        verify(meter, times(1)).markEvent();
    }

}
