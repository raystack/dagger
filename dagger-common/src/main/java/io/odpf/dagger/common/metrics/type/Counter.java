package io.odpf.dagger.common.metrics.type;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Counter extends Serializable {
    void increment(Aspects aspect);

    void decrement(Aspects aspect);
}
