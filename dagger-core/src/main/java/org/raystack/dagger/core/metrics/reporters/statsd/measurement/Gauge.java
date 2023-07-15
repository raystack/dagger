package org.raystack.dagger.core.metrics.reporters.statsd.measurement;

import org.raystack.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Gauge extends Serializable {
    void markValue(Aspects aspect, int gaugeValue);
}
