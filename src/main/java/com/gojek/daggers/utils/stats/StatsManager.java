package com.gojek.daggers.utils.stats;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;


public class StatsManager {
    private final HashMap<Aspects, Histogram> histogramMap;
    private RuntimeContext runtimeContext;
    private Boolean enabled;
    private HashMap<Aspects, Meter> meterMap;

    public StatsManager(RuntimeContext runtimeContext, Boolean enabled) {
        this.runtimeContext = runtimeContext;
        this.enabled = enabled;
        histogramMap = new HashMap<>();
        meterMap = new HashMap<>();
    }

    public void register(String groupName, Aspects[] aspects) {
        if (enabled) {
            MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup(groupName);
            register(metricGroup, aspects);
        }
    }

    private void register(MetricGroup metricGroup, Aspects[] aspects) {
        for (Aspects aspect : aspects) {
            if (AspectType.Histogram.equals(aspect.getAspectType()))
                histogramMap.put(aspect, metricGroup.histogram(aspect.getValue(), new DropwizardHistogramWrapper(getHistogram())));
            if (AspectType.Metric.equals(aspect.getAspectType()))
                meterMap.put(aspect, metricGroup.meter(aspect.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        }
    }

    private com.codahale.metrics.Histogram getHistogram() {
        return new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(10, TimeUnit.SECONDS));
    }

    public void updateHistogram(Aspects aspects, long value) {
        if (enabled)
            histogramMap.get(aspects).update(value);
    }

    public void markEvent(Aspects aspect) {
        if (enabled)
            meterMap.get(aspect).markEvent();
    }
}
