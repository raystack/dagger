package io.odpf.dagger.common.udfs.telemetry;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;

public class UdfMetricsManager {
    private FunctionContext context;
    private Integer gaugeValue = 1;
    private static final String UDF_TELEMETRY_GROUP_KEY = "udf";
    private static final String GAUGE_ASPECT_NAME = "value";

    public UdfMetricsManager(FunctionContext context) {
        this.context = context;
    }

    public void registerGauge(String udfValue) {
        MetricGroup metricGroup = context.getMetricGroup().addGroup(UDF_TELEMETRY_GROUP_KEY, udfValue);
        metricGroup.gauge(GAUGE_ASPECT_NAME, (Gauge<Integer>) () -> gaugeValue);
    }
}
