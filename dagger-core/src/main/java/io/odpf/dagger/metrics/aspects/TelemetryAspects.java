package io.odpf.dagger.metrics.aspects;

public enum TelemetryAspects implements Aspects {
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
