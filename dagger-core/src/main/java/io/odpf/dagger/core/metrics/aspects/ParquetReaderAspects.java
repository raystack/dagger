package io.odpf.dagger.core.metrics.aspects;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

public enum ParquetReaderAspects implements Aspects {
    READER_CREATED("reader_created", AspectType.Metric);

    private String value;
    private AspectType aspectType;

    ParquetReaderAspects(String value, AspectType aspectType) {
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
