package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.DoubleType;

/**
 * The Double value transformer.
 */
public class DoubleValueTransformer implements ValueTransformer {

    @Override
    public boolean canTransform(Object value) {
        return value instanceof Double;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return targetType == DoubleType;
    }

    @Override
    public Integer getIndex() {
        return DoubleType.getValue();
    }

    @Override
    public Object getValue(Object value) {
        return value != null ? Double.valueOf(value.toString()) : 0.0d;
    }
}
