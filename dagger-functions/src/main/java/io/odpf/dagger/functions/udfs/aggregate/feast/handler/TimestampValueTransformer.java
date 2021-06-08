package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.apache.flink.types.Row;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.TimestampType;

/**
 * The Timestamp value transformer.
 */
public class TimestampValueTransformer implements ValueTransformer {

    @Override
    public boolean canTransform(Object value) {
        return value instanceof Row && ((Row) value).getArity() == 2;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return value instanceof Row && ((Row) value).getArity() == 2 && targetType == ValueEnum.TimestampType;
    }

    @Override
    public Integer getIndex() {
        return TimestampType.getValue();
    }
}
