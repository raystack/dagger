package io.odpf.dagger.common.metrics.type;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Gauge extends Serializable {
    void registerLong(Aspects aspect, long gaugeValue);

    void registerString(Aspects aspect, String gaugeValue);

    void registerDouble(Aspects aspect, double gaugeValue);
}
