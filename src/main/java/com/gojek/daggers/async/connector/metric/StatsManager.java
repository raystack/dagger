package com.gojek.daggers.async.connector.metric;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.async.connector.metric.Aspects.*;

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

    public void register() {
        if (enabled) {
            MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup("es");
            registerHistograms(metricGroup);
            registerMeters(metricGroup);
        }
    }

    private void registerHistograms(MetricGroup metricGroup) {
        histogramMap.put(SUCCESS_RESPONSE_TIME, metricGroup
                .histogram(SUCCESS_RESPONSE_TIME.getValue(), new DropwizardHistogramWrapper(getHistogram())));
        histogramMap.put(FAILED_RESPONSE_TIME, metricGroup
                .histogram(FAILED_RESPONSE_TIME.getValue(), new DropwizardHistogramWrapper(getHistogram())));
    }

    private void registerMeters(MetricGroup metricGroup) {
        meterMap.put(TOTAL_CALLS, metricGroup.meter(TOTAL_CALLS.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(FOUR_XX_FAILURES, metricGroup.meter(FOUR_XX_FAILURES.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(SUCCESSES, metricGroup.meter(SUCCESSES.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(FAILURES, metricGroup.meter(FAILURES.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(FIVE_XX_FAILURES, metricGroup.meter(FIVE_XX_FAILURES.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
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
