package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import io.odpf.dagger.common.metrics.aspects.Aspects;

/**
 * The Gauge stats manager.
 */
public class GaugeStatsManager {

    private final Boolean enabled;
    private final MetricGroup metricGroup;

    /**
     * Instantiates a new Gauge stats manager.
     *
     * @param metricGroup the metric group
     * @param enabled     the enabled
     */
    public GaugeStatsManager(MetricGroup metricGroup, Boolean enabled) {
        this.metricGroup = metricGroup;
        this.enabled = enabled;
    }

    /**
     * Register aspects.
     *
     * @param groupKey   the group key
     * @param groupValue the group value
     * @param aspects    the aspects
     * @param gaugeValue the gauge value
     */
    public void registerAspects(String groupKey, String groupValue, Aspects[] aspects, int gaugeValue) {
        if (enabled) {
            for (Aspects aspect : aspects) {
                metricGroup.addGroup(groupKey, groupValue).gauge(aspect.getValue(), (Gauge<Integer>) () -> gaugeValue);
            }
        }
    }

    /**
     * Register integer gauge aspect.
     *
     * @param groupKey        the group key
     * @param groupValue      the group value
     * @param gaugeAspectName the gauge aspect name
     * @param gaugeValue      the gauge value
     */
    public void registerInteger(String groupKey, String groupValue, String gaugeAspectName, int gaugeValue) {
        if (enabled) {
            metricGroup.addGroup(groupKey, groupValue).gauge(gaugeAspectName, (Gauge<Integer>) () -> gaugeValue);
        }
    }

    /**
     * Register string gauge aspect.
     *
     * @param groupKey        the group key
     * @param groupValue      the group value
     * @param gaugeAspectName the gauge aspect name
     * @param gaugeValue      the gauge value
     */
    public void registerString(String groupKey, String groupValue, String gaugeAspectName, String gaugeValue) {
        if (enabled) {
            metricGroup.addGroup(groupKey, groupValue).gauge(gaugeAspectName, (Gauge<String>) () -> gaugeValue);
        }
    }

    /**
     * Register double gauge aspect.
     *
     * @param groupKey        the group key
     * @param groupValue      the group value
     * @param gaugeAspectName the gauge aspect name
     * @param gaugeValue      the gauge value
     */
    public void registerDouble(String groupKey, String groupValue, String gaugeAspectName, Double gaugeValue) {
        if (enabled) {
            metricGroup.addGroup(groupKey, groupValue).gauge(gaugeAspectName, (Gauge<Double>) () -> gaugeValue);
        }
    }
}
