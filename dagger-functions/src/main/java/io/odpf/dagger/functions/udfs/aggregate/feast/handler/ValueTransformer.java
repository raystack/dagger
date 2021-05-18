package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import org.apache.flink.types.Row;

import static io.odpf.dagger.functions.common.Constants.NUMBER_OF_DATA_TYPES_IN_FEATURE_ROW;

public interface ValueTransformer {
    boolean canTransform(Object value);

    boolean canTransformWithTargetType(Object value, ValueEnum targetType);

    Integer getIndex();

    default Object getValue(Object value) {
        return value;
    }

    default Row transform(Object value) {
        Row row = new Row(NUMBER_OF_DATA_TYPES_IN_FEATURE_ROW);
        row.setField(getIndex(), getValue(value));
        return row;
    }
}
