package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.BooleanType;

/**
 * The Boolean value transformer.
 */
public class BooleanValueTransformer implements ValueTransformer {
    @Override
    public boolean canTransform(Object value) {
        return value instanceof Boolean;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return targetType == BooleanType;
    }

    @Override
    public Integer getIndex() {
        return BooleanType.getValue();
    }

    @Override
    public Object getValue(Object value) {
        return value != null ? value : false;
    }
}
