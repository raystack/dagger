package com.gotocompany.dagger.core.metrics.reporters.statsd.measurement;

import com.gotocompany.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;

public interface Gauge extends Serializable {
    void markValue(Aspects aspect, int gaugeValue);
}
