package com.gojek.daggers.async.connector.metric;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.async.connector.metric.Aspects.*;

public class StatsManager {
    private final HashMap<Aspects, Histogram> histogramMap;
    private final String groupName;
    private RuntimeContext runtimeContext;
    private Boolean enabled;
    private HashMap<Aspects, Meter> meterMap;

    public StatsManager(RuntimeContext runtimeContext, String groupName, Boolean enabled) {
        this.runtimeContext = runtimeContext;
        this.groupName = groupName;
        this.enabled = enabled;
        histogramMap = new HashMap<>();
        meterMap = new HashMap<>();
    }

    public void register() {
        if (enabled) {
            MetricGroup metricGroup = runtimeContext.getMetricGroup().addGroup(groupName);
            registerHistograms(metricGroup);
            registerMeters(metricGroup);
        }
    }

    private void registerHistograms(MetricGroup metricGroup) {
        histogramMap.put(SUCCESS_RESPONSE_TIME, metricGroup
                .histogram(SUCCESS_RESPONSE_TIME.getValue(), new DropwizardHistogramWrapper(getHistogram())));
        histogramMap.put(OTHER_ERRORS_RESPONSE_TIME, metricGroup
                .histogram(OTHER_ERRORS_RESPONSE_TIME.getValue(), new DropwizardHistogramWrapper(getHistogram())));
        histogramMap.put(REQUEST_ERRORS_RESPONSE_TIME, metricGroup
                .histogram(REQUEST_ERRORS_RESPONSE_TIME.getValue(), new DropwizardHistogramWrapper(getHistogram())));
        histogramMap.put(FAILURES_ON_ES_RESPONSE_TIME, metricGroup
                .histogram(FAILURES_ON_ES_RESPONSE_TIME.getValue(), new DropwizardHistogramWrapper(getHistogram())));
    }

    private void registerMeters(MetricGroup metricGroup) {
        meterMap.put(TOTAL_ES_CALLS, metricGroup.meter(TOTAL_ES_CALLS.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(DOCUMENT_FOUND, metricGroup.meter(DOCUMENT_FOUND.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(ERROR_PARSING_RESPONSE, metricGroup.meter(ERROR_PARSING_RESPONSE.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(FAILURES_ON_ES, metricGroup.meter(FAILURES_ON_ES.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(REQUEST_ERROR, metricGroup.meter(REQUEST_ERROR.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(TOTAL_FAILED_REQUESTS, metricGroup.meter(TOTAL_FAILED_REQUESTS.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(OTHER_ERRORS, metricGroup.meter(OTHER_ERRORS.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(DOCUMENT_NOT_FOUND_ON_ES, metricGroup.meter(DOCUMENT_NOT_FOUND_ON_ES.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(EMPTY_INPUT, metricGroup.meter(EMPTY_INPUT.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(OTHER_ERRORS_PROCESSING_RESPONSE, metricGroup.meter(OTHER_ERRORS_PROCESSING_RESPONSE.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
        meterMap.put(ERROR_READING_RESPONSE, metricGroup.meter(ERROR_READING_RESPONSE.getValue(), new DropwizardMeterWrapper(new com.codahale.metrics.Meter())));
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
