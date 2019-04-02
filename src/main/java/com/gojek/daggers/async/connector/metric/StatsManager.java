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
    private HashMap<Aspects, Counter> counterMap;
    private HashMap<Aspects, Meter> meterMap;

    public StatsManager(RuntimeContext runtimeContext, Boolean enabled) {
        this.runtimeContext = runtimeContext;
        this.enabled = enabled;
        counterMap = new HashMap<>();
        histogramMap = new HashMap<>();
        meterMap = new HashMap<>();
    }

    public void register() {
        if (enabled) {
            MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup("es");
            registerCounters(metricGroup);
            registerHistograms(metricGroup);
        }
    }

    private void registerHistograms(MetricGroup metricGroup) {
        histogramMap.put(SUCCESS_RESPONSE_TIME, metricGroup
                .histogram(SUCCESS_RESPONSE_TIME.getValue(), new DropwizardHistogramWrapper(getHistogram())));
        histogramMap.put(FAILED_RESPONSE_TIME, metricGroup
                .histogram(FAILED_RESPONSE_TIME.getValue(), new DropwizardHistogramWrapper(getHistogram())));
    }

    private void registerCounters(MetricGroup metricGroup) {
        counterMap.put(SUCCESS_RESPONSE, metricGroup.counter(SUCCESS_RESPONSE.getValue()));
        counterMap.put(EXCEPTION, metricGroup.counter(EXCEPTION.getValue()));
        counterMap.put(FIVE_XX_RESPONSE, metricGroup.counter(FIVE_XX_RESPONSE.getValue()));
        counterMap.put(CALL_COUNT, metricGroup.counter(CALL_COUNT.getValue()));
        counterMap.put(FOUR_XX_RESPONSE, metricGroup.counter(FOUR_XX_RESPONSE.getValue()));
    }

    private com.codahale.metrics.Histogram getHistogram() {
        return new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(10, TimeUnit.SECONDS));
    }

    public void incCounter(Aspects aspect) {
        if (enabled)
            counterMap.get(aspect).inc();
    }

    public void updateHistogram(Aspects aspects, long value) {
        if (enabled)
            histogramMap.get(aspects).update(value);
    }

}
