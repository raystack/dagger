package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static io.odpf.dagger.common.core.Constants.SLIDING_TIME_WINDOW;


public class MeterStatsManager {
    private final HashMap<Aspects, Histogram> histogramMap;
    private Boolean enabled;
    private HashMap<Aspects, Meter> meterMap;
    private MetricGroup metricGroup;

    public MeterStatsManager(MetricGroup metricGroup, Boolean enabled) {
        this.metricGroup = metricGroup;
        this.enabled = enabled;
        histogramMap = new HashMap<>();
        meterMap = new HashMap<>();
    }

    public MeterStatsManager(MetricGroup metricGroup, Boolean enabled, HashMap histogramMap, HashMap meterMap) {
        this.metricGroup = metricGroup;
        this.enabled = enabled;
        this.histogramMap = histogramMap;
        this.meterMap = meterMap;
    }

    public void register(String groupName, Aspects[] aspects) {
        if (enabled) {
            register(metricGroup.addGroup(groupName), aspects);
        }
    }

    private com.codahale.metrics.Histogram getHistogram() {
        return new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(SLIDING_TIME_WINDOW, TimeUnit.SECONDS));
    }

    public void updateHistogram(Aspects aspects, long value) {
        if (enabled) {
            histogramMap.get(aspects).update(value);
        }
    }

    public void markEvent(Aspects aspect) {
        if (enabled) {
            meterMap.get(aspect).markEvent();
        }
    }

    public void register(String groupKey, String groupValue, Aspects[] aspects) {
        if (enabled) {
            register(metricGroup.addGroup(groupKey, groupValue), aspects);
        }
    }

    private void register(MetricGroup group, Aspects[] aspects) {
        for (Aspects aspect : aspects) {
            if (AspectType.Histogram.equals(aspect.getAspectType())) {
                histogramMap.put(aspect, group.histogram(aspect.getValue(), new DropwizardHistogramWrapper(getHistogram())));
            }
            if (AspectType.Metric.equals(aspect.getAspectType())) {
                meterMap.put(aspect, group.meter(aspect.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
            }
        }
    }
}
