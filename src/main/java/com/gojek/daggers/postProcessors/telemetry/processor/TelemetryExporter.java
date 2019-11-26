package com.gojek.daggers.postProcessors.telemetry.processor;

import com.gojek.daggers.metrics.GaugeStatsManager;
import com.gojek.daggers.metrics.TelemetryPublisher;
import com.gojek.daggers.metrics.TelemetrySubscriber;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.metrics.TelemetryAspects.values;

public class TelemetryExporter extends RichMapFunction<Row, Row> implements TelemetrySubscriber {
    private GaugeStatsManager gaugeStatsManager;
    private Integer gaugeValue = 1;
    private Map<String, List<String>> metrics = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        if (gaugeStatsManager == null)
            gaugeStatsManager = new GaugeStatsManager(getRuntimeContext(), true);
        if(metrics!= null)
            registerGroups(gaugeStatsManager);
    }

    @Override
    public Row map(Row inputRow) throws Exception {
        return inputRow;
    }

    private void registerGroups(GaugeStatsManager gaugeStatsManager) {
        metrics.forEach((groupKey, groupValues) -> groupValues
                .forEach(groupValue -> gaugeStatsManager.register(groupKey, groupValue, values(), gaugeValue)));
    }

    @Override
    public void updated(TelemetryPublisher publisher) {
        metrics.putAll(publisher.getTelemetry());
    }
}
