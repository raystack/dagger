package io.odpf.dagger.common.metrics.type;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Gauge extends Serializable {
    void registerInteger(Aspects aspect, int gaugeValue);

    void registerString(Aspects aspect, String gaugeValue);

    void registerDouble(Aspects aspect, double gaugeValue);
}
