package com.gotocompany.dagger.core.metrics.reporters.statsd.manager;

import com.gotocompany.dagger.core.metrics.reporters.statsd.tags.StatsDTag;

import java.io.Serializable;

public interface MeasurementManager extends Serializable {
    void register(StatsDTag[] tags);
}
