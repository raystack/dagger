package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import java.math.BigDecimal;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.DoubleType;

/**
 * The Big decimal value transformer.
 */
public class BigDecimalValueTransformer implements ValueTransformer {
    @Override
    public boolean canTransform(Object value) {
        return value instanceof BigDecimal;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return value instanceof BigDecimal && targetType == DoubleType;
    }

    @Override
    public Integer getIndex() {
        return DoubleType.getValue();
    }

    @Override
    public Object getValue(Object value) {
        if (value == null) {
            return 0.0D;
        }
        BigDecimal bigDecimalValue = (BigDecimal) value;
        return bigDecimalValue.doubleValue();
    }
}
