package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.util.HashMap;
import java.util.Map;

public class CounterStatsManager {
    private MetricGroup metricGroup;
    private Map<Aspects, Counter> counters = new HashMap<>();

    public CounterStatsManager(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void inc(Aspects aspect) {
        counters.get(aspect).inc();
    }

    public long getCount(Aspects aspect) {
        return counters.get(aspect).getCount();
    }

    public void registerAspects(Aspects[] aspects, String groupName) {
        for (Aspects aspect : aspects) {
            register(aspect, groupName);
        }
    }

    public void register(Aspects aspect, String groupName) {
        if (aspect.getAspectType() == AspectType.Counter) {
            counters.put(aspect, metricGroup.addGroup(groupName).counter(aspect.getValue()));
        }
    }
}
