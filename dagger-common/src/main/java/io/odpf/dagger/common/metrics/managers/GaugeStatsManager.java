package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.common.metrics.aspects.Aspects;

public class GaugeStatsManager {

    private final Boolean enabled;
    private final MetricGroup metricGroup;

    public GaugeStatsManager(MetricGroup metricGroup, Boolean enabled) {
        this.metricGroup = metricGroup;
        this.enabled = enabled;
    }

    public void registerAspects(String groupKey, String groupValue, Aspects[] aspects, int gaugeValue) {
        if (enabled) {
            for (Aspects aspect : aspects) {
                metricGroup.addGroup(groupKey, groupValue).gauge(aspect.getValue(), (Gauge<Integer>) () -> gaugeValue);
            }
        }
    }

    public void register(String groupKey, String groupValue, String gaugeAspectName, int gaugeValue) {
        if (enabled) {
            metricGroup.addGroup(groupKey, groupValue).gauge(gaugeAspectName, (Gauge<Integer>) () -> gaugeValue);
        }
    }
}
