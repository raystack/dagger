package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.IntegerType;

/**
 * The Integer value transformer.
 */
public class IntegerValueTransformer implements ValueTransformer {


    @Override
    public boolean canTransform(Object value) {
        return value instanceof Integer;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return targetType == IntegerType;
    }

    @Override
    public Integer getIndex() {
        return IntegerType.getValue();
    }

    @Override
    public Object getValue(Object value) {
        return value != null ? Integer.valueOf(value.toString()) : 0;
    }
}
