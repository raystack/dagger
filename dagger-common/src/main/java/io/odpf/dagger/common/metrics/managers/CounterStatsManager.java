package io.odpf.dagger.common.metrics.managers;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;

/**
 * The Counter stats manager.
 */
public class CounterStatsManager {
    private MetricGroup metricGroup;
    private Map<Aspects, Counter> counters = new HashMap<>();

    /**
     * Instantiates a new Counter stats manager.
     *
     * @param metricGroup the metric group
     */
    public CounterStatsManager(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    /**
     * Increment the aspect.
     *
     * @param aspect the aspect
     */
    public void inc(Aspects aspect) {
        counters.get(aspect).inc();
    }

    /**
     * Get aspect count.
     *
     * @param aspect the aspect
     * @return the count
     */
    public long getCount(Aspects aspect) {
        return counters.get(aspect).getCount();
    }

    /**
     * Register aspects.
     *
     * @param aspects   the aspects
     * @param groupName the group name
     */
    public void registerAspects(Aspects[] aspects, String groupName) {
        for (Aspects aspect : aspects) {
            register(aspect, groupName);
        }
    }

    /**
     * Register aspect to metric group.
     *
     * @param aspect    the aspect
     * @param groupName the group name
     */
    public void register(Aspects aspect, String groupName) {
        if (aspect.getAspectType() == AspectType.Counter) {
            Counter counter = metricGroup.addGroup(groupName).counter(aspect.getValue());
            counters.put(aspect, counter);
        }
    }

    /**
     * Register aspect to metric group with specified groupKey and groupValue pair.
     *
     * @param aspect     the aspect
     * @param groupKey   the group key
     * @param groupValue the group value
     */
    public void register(Aspects aspect, String groupKey, String groupValue) {
        if (aspect.getAspectType() == AspectType.Counter) {
            Counter counter = metricGroup.addGroup(groupKey, groupValue).counter(aspect.getValue());
            counters.put(aspect, counter);
        }
    }
}
