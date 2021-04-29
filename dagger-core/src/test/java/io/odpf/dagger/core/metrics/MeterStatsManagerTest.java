package io.odpf.dagger.core.metrics;

import io.odpf.dagger.core.metrics.aspects.LongbowWriterAspects;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;

import static org.mockito.Mockito.*;
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

        meterStatsManager.register(groupName, LongbowWriterAspects.values());
        verify(metricGroup, times(4)).histogram(any(String.class), any(Histogram.class));
    }

    @Test
    public void shouldRegisterMeterAspects() {
        String groupName = "test_groupName";
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        when(metricGroup.addGroup(groupName)).thenReturn(metricGroup);

        meterStatsManager.register(groupName, LongbowWriterAspects.values());
        verify(metricGroup, times(6)).meter(any(String.class), any(DropwizardMeterWrapper.class));
    }

    @Test
    public void shouldUpdateHistogram() {
        meterStatsManager = new MeterStatsManager(runtimeContext, true, histogramMap, meterMap);
        when(histogramMap.get(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME)).thenReturn(histogram);

        meterStatsManager.updateHistogram(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE_RESPONSE_TIME, 100);
        verify(histogram, times(1)).update(100);
    }

    @Test
    public void shouldMarkMeterEvents() {
        meterStatsManager = new MeterStatsManager(runtimeContext, true, histogramMap, meterMap);
        when(meterMap.get(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE)).thenReturn(meter);

        meterStatsManager.markEvent(LongbowWriterAspects.SUCCESS_ON_CREATE_BIGTABLE);
        verify(meter, times(1)).markEvent();
    }
}