package io.odpf.dagger.common.metrics.type;

import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.type.statsd.tags.StatsDTag;

import java.io.Serializable;

public interface MeasurementManager extends Serializable {
    void register(Aspects[] aspect, StatsDTag[] tags);
    void register(StatsDTag[] tags);
}
