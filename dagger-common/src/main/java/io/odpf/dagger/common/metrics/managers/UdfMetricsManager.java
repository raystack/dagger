package io.odpf.dagger.common.metrics.managers;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

public class UdfMetricsManager {
    private Integer gaugeValue = 1;
    private static final String UDF_TELEMETRY_GROUP_KEY = "udf";
    private static final String GAUGE_ASPECT_NAME = "value";
    private MetricGroup metricGroup;

    public UdfMetricsManager(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerGauge(String udfValue) {
        MetricGroup addedGroup = this.metricGroup.addGroup(UDF_TELEMETRY_GROUP_KEY, udfValue);
        addedGroup.gauge(GAUGE_ASPECT_NAME, (Gauge<Integer>) () -> gaugeValue);
    }
}
