package org.raystack.dagger.core.metrics.reporters.statsd.measurement;

import org.raystack.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Counter extends Serializable {
    void increment(Aspects aspect);

    void increment(Aspects aspect, long num);

    void decrement(Aspects aspect);

    void decrement(Aspects aspect, long num);
}
