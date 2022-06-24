package io.odpf.dagger.common.metrics.type.statsd;

import io.odpf.dagger.common.metrics.aspects.Aspects;
import io.odpf.dagger.common.metrics.type.Gauge;
import io.odpf.dagger.common.metrics.type.MeasurementManager;

import java.util.Map;

public class DaggerGaugeManager implements MeasurementManager, Gauge {
    @Override
    public void registerInteger(Aspects aspect, int gaugeValue) {

    }

    @Override
    public void registerString(Aspects aspect, String gaugeValue) {

    }

    @Override
    public void registerDouble(Aspects aspect, double gaugeValue) {

    }

    @Override
    public void register(Aspects[] aspect, Map<String, String> tagKeyValuePairs) {

    }
}
