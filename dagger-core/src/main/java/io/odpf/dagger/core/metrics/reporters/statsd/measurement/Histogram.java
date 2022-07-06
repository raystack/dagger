package io.odpf.dagger.core.metrics.reporters.statsd.measurement;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Histogram extends Serializable {
    void recordValue(Aspects aspect, long value);
}
