package io.odpf.dagger.common.metrics.type.statsd;

import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.type.MeasurementManager;
import io.odpf.dagger.common.metrics.type.Meter;

import java.util.Map;

public class DaggerMeterManager implements MeasurementManager, Meter {
    @Override
    public void register(Aspects[] aspect, Map<String, String> tagKeyValuePairs) {

    }

    @Override
    public void markEvent(Aspects aspect) {

    }

    @Override
    public void markEvent(Aspects aspect, long num) {

    }
}
