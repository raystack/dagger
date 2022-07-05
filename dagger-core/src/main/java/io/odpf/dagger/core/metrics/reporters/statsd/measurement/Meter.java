package io.odpf.dagger.core.metrics.reporters.statsd.measurement;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Meter extends Serializable {
    void markEvent(Aspects aspect);

    void markEvent(Aspects aspect, long num);
}
