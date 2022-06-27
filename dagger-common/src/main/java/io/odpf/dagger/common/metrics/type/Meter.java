package io.odpf.dagger.common.metrics.type;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Meter extends Serializable {
    void markEvent(Aspects aspect);

    void markEvent(Aspects aspect, long num);
}
