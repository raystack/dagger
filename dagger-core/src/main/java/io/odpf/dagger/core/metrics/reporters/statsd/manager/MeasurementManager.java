package io.odpf.dagger.core.metrics.reporters.statsd.manager;

import io.odpf.dagger.core.metrics.reporters.statsd.tags.StatsDTag;

import java.io.Serializable;

public interface MeasurementManager extends Serializable {
    void register(StatsDTag[] tags);
}
