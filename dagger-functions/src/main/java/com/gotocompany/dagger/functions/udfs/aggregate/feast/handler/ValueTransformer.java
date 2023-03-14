package com.gotocompany.dagger.functions.udfs.aggregate.feast.handler;

import com.gotocompany.dagger.functions.common.Constants;
import org.apache.flink.types.Row;

/**
 * The interface Value transformer.
 */
public interface ValueTransformer {
    /**
     * Check if can transform.
     *
     * @param value the value
     * @return the boolean
     */
    boolean canTransform(Object value);

    /**
     * Check if can transform with target type.
     *
     * @param value      the value
     * @param targetType the target type
     * @return the boolean
     */
    boolean canTransformWithTargetType(Object value, ValueEnum targetType);

    /**
     * Gets index.
     *
     * @return the index
     */
    Integer getIndex();

    /**
     * Gets value.
     *
     * @param value the value
     * @return the value
     */
    default Object getValue(Object value) {
        return value;
    }

    /**
     * Transform row.
     *
     * @param value the value
     * @return the row
     */
    default Row transform(Object value) {
        Row row = new Row(Constants.NUMBER_OF_DATA_TYPES_IN_FEATURE_ROW);
        row.setField(getIndex(), getValue(value));
        return row;
    }
}
