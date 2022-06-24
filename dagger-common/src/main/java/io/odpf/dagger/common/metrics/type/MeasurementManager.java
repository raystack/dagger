package io.odpf.dagger.common.metrics.type;

import io.odpf.dagger.common.metrics.aspects.Aspects;

import java.io.Serializable;
import java.util.Map;

public interface MeasurementManager extends Serializable {
    void register(Aspects[] aspect, Map<String, String> tagKeyValuePairs);
}
