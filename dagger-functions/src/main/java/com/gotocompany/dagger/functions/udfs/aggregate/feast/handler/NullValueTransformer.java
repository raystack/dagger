package com.gotocompany.dagger.functions.udfs.aggregate.feast.handler;

import com.gotocompany.dagger.functions.common.Constants;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.types.Row;

/**
 * The Null value transformer.
 */
public class NullValueTransformer implements ValueTransformer {

    @Override
    public boolean canTransform(Object value) {
        return null == value;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return null == value;
    }

    @Override
    public Integer getIndex() {
        throw new NotImplementedException("Index for Null Value shouldn't be used");
    }

    @Override
    public Row transform(Object value) {
        return new Row(Constants.NUMBER_OF_DATA_TYPES_IN_FEATURE_ROW);
    }
}
