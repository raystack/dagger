package com.gojek.daggers.async.metric;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.async.metric.Aspects.*;

public class StatsManager {
    private final HashMap<Aspects, Histogram> histogramMap;
    private final String groupName;
    private RuntimeContext runtimeContext;
    private Boolean enabled;
    private HashMap<Aspects, Meter> meterMap;

    public StatsManager(RuntimeContext runtimeContext, String groupName, Boolean enabled) {
        this.runtimeContext = runtimeContext;
        this.groupName = groupName;
        this.enabled = enabled;
        histogramMap = new HashMap<>();
        meterMap = new HashMap<>();
    }

    public void register() {
        if (enabled) {
            MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup(groupName);
            register(metricGroup);
        }
    }

    private void register(MetricGroup metricGroup) {
        for (Aspects aspect : values()) {
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
