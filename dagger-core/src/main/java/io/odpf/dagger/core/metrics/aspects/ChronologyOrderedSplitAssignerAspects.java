package io.odpf.dagger.core.metrics.aspects;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

public enum ChronologyOrderedSplitAssignerAspects implements Aspects {
    TOTAL_SPLITS_DISCOVERED("total_splits_discovered", AspectType.Gauge),
    TOTAL_SPLITS_RECORDED("total_splits_recorded", AspectType.Gauge),
    SPLITS_AWAITING_ASSIGNMENT("splits_awaiting_assignment", AspectType.Counter);

    ChronologyOrderedSplitAssignerAspects(String value, AspectType aspectType) {
        this.value = value;
        this.aspectType = aspectType;
    }

    private final String value;
    private final AspectType aspectType;

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public AspectType getAspectType() {
        return aspectType;
    }
}
