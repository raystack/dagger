package com.gojek.daggers.postProcessors.telemetry.processor;

import com.gojek.daggers.metrics.GaugeStatsManager;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.gojek.daggers.metrics.aspects.TelemetryAspects.values;

public class MetricsTelemetryExporter extends RichMapFunction<Row, Row> implements TelemetrySubscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsTelemetryExporter.class.getName());
    private GaugeStatsManager gaugeStatsManager;
    private Integer gaugeValue = 1;
    protected Map<String, Set<String>> metrics = new HashMap<>();

    public MetricsTelemetryExporter(GaugeStatsManager gaugeStatsManager) {
        this.gaugeStatsManager = gaugeStatsManager;
    }

    public MetricsTelemetryExporter() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (gaugeStatsManager == null) {
            gaugeStatsManager = new GaugeStatsManager(getRuntimeContext(), true);
        }
        if (metrics != null) {
            registerGroups(gaugeStatsManager);
        }
    }

    @Override
    public Row map(Row inputRow) throws Exception {
        return inputRow;
    }

    @Override
    public void updated(TelemetryPublisher publisher) {
        mergeMetrics(publisher.getTelemetry());
        if (gaugeStatsManager != null) {
            registerGroups(gaugeStatsManager);
        }
    }

    private void mergeMetrics(Map<String, List<String>> metricsFromPublisher) {
        metricsFromPublisher.forEach((key, value) -> {
                    metrics.computeIfAbsent(key, x -> new HashSet<>()).addAll(value);
                }
        );
    }

    protected void registerGroups(GaugeStatsManager gaugeStatsManager) {
        metrics.forEach((groupKey, groupValues) -> groupValues
                .forEach(groupValue -> gaugeStatsManager.register(groupKey, groupValue, values(), gaugeValue)));
        LOGGER.info("Sending Metrics: " + metrics);
        metrics.clear();
    }
}
