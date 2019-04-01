package com.gojek.daggers.async.connector.metric;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.SlidingWindowReservoir;
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
    private HashMap<Aspects, Counter> counterMap;
    private HashMap<Aspects, Meter> meterMap;

    public StatsManager(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
        counterMap = new HashMap<>();
        histogramMap = new HashMap<>();
        meterMap = new HashMap<>();
    }

    public void register() {
        MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup("es");
        registerCounters(metricGroup);
        registerHistograms(metricGroup);
        registerMeters(metricGroup);
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
    }

    private void registerMeters(MetricGroup metricGroup) {
        meterMap.put(CALL_COUNT, metricGroup.meter(CALL_COUNT.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(FOUR_XX_RESPONSE, metricGroup.meter(FOUR_XX_RESPONSE.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
    }

    private com.codahale.metrics.Histogram getHistogram() {
        return new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(10, TimeUnit.SECONDS));
    }

    public Counter getCounter(Aspects aspect) {
        return counterMap.get(aspect);
    }

    public Histogram getHistogram(Aspects aspects) {
        return histogramMap.get(aspects);
    }

    public Meter getMeter(Aspects aspect) {
        return meterMap.get(aspect);
    }
}
