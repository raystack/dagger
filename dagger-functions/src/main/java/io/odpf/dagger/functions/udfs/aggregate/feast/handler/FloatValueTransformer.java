package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.FloatType;

/**
 * The Float value transformer.
 */
public class FloatValueTransformer implements ValueTransformer {
    @Override
    public boolean canTransform(Object value) {
        return value instanceof Float;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return targetType == FloatType;
    }

    @Override
    public Integer getIndex() {
        return FloatType.getValue();
    }

    @Override
    public Object getValue(Object value) {
        return value != null ? Float.valueOf(value.toString()) : 0.0f;
    }
}
