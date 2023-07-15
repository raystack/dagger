package org.raystack.dagger.functions.transformers.filter;

import org.raystack.dagger.common.metrics.aspects.AspectType;
import org.raystack.dagger.common.metrics.aspects.Aspects;

/**
 * The enum Filter aspects.
 */
public enum FilterAspects implements Aspects {
    /**
     * Filtered invalid records filter aspects.
     */
    FILTERED_INVALID_RECORDS("filtered_invalid_records", AspectType.Counter);

    FilterAspects(String value, AspectType type) {
        this.value = value;
        this.type = type;
    }

    private String value;
    private AspectType type;

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public AspectType getAspectType() {
        return this.type;
    }
}
