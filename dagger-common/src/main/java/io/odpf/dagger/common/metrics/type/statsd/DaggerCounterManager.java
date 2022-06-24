package io.odpf.dagger.common.metrics.type.statsd;

import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.type.Counter;
import io.odpf.dagger.common.metrics.type.MeasurementManager;

import java.util.Map;

public class DaggerCounterManager implements MeasurementManager, Counter {
    @Override
    public void increment(Aspects aspect) {

    }

    @Override
    public void decrement(Aspects aspect) {

    }

    @Override
    public void register(Aspects[] aspect, Map<String, String> tagKeyValuePairs) {

    }
}
