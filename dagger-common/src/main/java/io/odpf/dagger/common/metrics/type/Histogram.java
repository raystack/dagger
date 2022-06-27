package io.odpf.dagger.common.metrics.type;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Histogram extends Serializable {
    void recordValue(Aspects aspect, long value);

    void recordValue(Aspects aspect, double value);
}
