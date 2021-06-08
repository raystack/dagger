package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.StringType;

/**
 * The String value transformer.
 */
public class StringValueTransformer implements ValueTransformer {

    @Override
    public boolean canTransform(Object value) {
        return value instanceof String;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return targetType == StringType;
    }

    @Override
    public Integer getIndex() {
        return StringType.getValue();
    }

    @Override
    public Object getValue(Object value) {
        return value.toString();
    }
}
