package io.odpf.dagger.core.metrics.aspects;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

/**
 * The enum Telemetry aspects.
 */
public enum TelemetryAspects implements Aspects {
    /**
     * Value telemetry aspects.
     */
    VALUE("value", AspectType.Metric);

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
