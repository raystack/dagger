package org.raystack.dagger.core.metrics.reporters.statsd.measurement;

import org.raystack.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Histogram extends Serializable {
    void recordValue(Aspects aspect, long value);
}
