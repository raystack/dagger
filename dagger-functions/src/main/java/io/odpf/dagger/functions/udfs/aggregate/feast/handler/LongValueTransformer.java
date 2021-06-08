package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.LongType;

/**
 * The Long value transformer.
 */
public class LongValueTransformer implements ValueTransformer {

    @Override
    public boolean canTransform(Object value) {
        return value instanceof Long;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return targetType == LongType;
    }

    @Override
    public Integer getIndex() {
        return LongType.getValue();
    }

    @Override
    public Object getValue(Object value) {
        return value != null ? Long.valueOf(value.toString()) : 0L;
    }
}
