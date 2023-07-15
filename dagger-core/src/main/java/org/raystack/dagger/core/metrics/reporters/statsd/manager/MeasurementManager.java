package org.raystack.dagger.core.metrics.reporters.statsd.manager;

import org.raystack.dagger.core.metrics.reporters.statsd.tags.StatsDTag;

import java.io.Serializable;

public interface MeasurementManager extends Serializable {
    void register(StatsDTag[] tags);
}
