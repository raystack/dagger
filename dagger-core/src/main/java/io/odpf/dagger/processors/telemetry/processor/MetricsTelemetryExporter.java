package io.odpf.dagger.processors.telemetry.processor;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import io.odpf.dagger.common.contracts.TelemetryPublisher;
import io.odpf.dagger.common.contracts.TelemetrySubscriber;
import io.odpf.dagger.metrics.GaugeStatsManager;
import io.odpf.dagger.metrics.aspects.TelemetryAspects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
                .forEach(groupValue -> gaugeStatsManager.register(groupKey, groupValue, TelemetryAspects.values(), gaugeValue)));
        LOGGER.info("Sending Metrics: " + metrics);
        metrics.clear();
    }
}
