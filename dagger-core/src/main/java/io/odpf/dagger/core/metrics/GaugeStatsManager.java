package io.odpf.dagger.core.metrics;

import io.odpf.dagger.core.metrics.aspects.Aspects;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

public class GaugeStatsManager {

    private final RuntimeContext runtimeContext;
    private final Boolean enabled;

    public GaugeStatsManager(RuntimeContext runtimeContext, Boolean enabled) {
        this.runtimeContext = runtimeContext;
        this.enabled = enabled;
    }

    public void register(String groupKey, String groupValue, Aspects[] aspects, int gaugeValue) {
        if (enabled) {
            MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup(groupKey, groupValue);
            register(metricGroup, aspects, gaugeValue);
        }
    }

    private void register(MetricGroup metricGroup, Aspects[] aspects, int gaugeValue) {
        for (Aspects aspect : aspects) {
            metricGroup.gauge(aspect.getValue(), (Gauge<Integer>) () -> gaugeValue);
        }
    }
}
