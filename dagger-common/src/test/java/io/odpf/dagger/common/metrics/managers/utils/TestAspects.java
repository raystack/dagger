package io.odpf.dagger.common.metrics.managers.utils;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

public enum TestAspects implements Aspects {
    TEST_ASPECT_ONE("test_aspect1", AspectType.Histogram),
    TEST_ASPECT_TWO("test_aspect2", AspectType.Metric),
    TEST_ASPECT_THREE("test_aspect3", AspectType.Counter);

    private String value;
    private AspectType aspectType;

    TestAspects(String value, AspectType aspectType) {
        this.value = value;
        this.aspectType = aspectType;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public AspectType getAspectType() {
        return aspectType;
    }
}
