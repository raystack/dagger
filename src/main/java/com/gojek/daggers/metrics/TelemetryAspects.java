package com.gojek.daggers.metrics;

import com.gojek.daggers.metrics.aspects.AspectType;
import com.gojek.daggers.metrics.aspects.Aspects;

import static com.gojek.daggers.metrics.aspects.AspectType.Metric;

public enum TelemetryAspects implements Aspects {
    VALUE("value", Metric);

    private String value;
    private AspectType aspectType;

    TelemetryAspects(String value, AspectType aspectType) {
        this.value = value;
        this.aspectType = aspectType;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public AspectType getAspectType() {
        return aspectType;
    }
}
