package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import com.google.protobuf.ByteString;

import static io.odpf.dagger.functions.udfs.aggregate.feast.handler.ValueEnum.ByteType;

/**
 * The Byte value transformer.
 */
public class ByteValueTransformer implements ValueTransformer {
    @Override
    public boolean canTransform(Object value) {
        return value instanceof ByteString;
    }

    @Override
    public boolean canTransformWithTargetType(Object value, ValueEnum targetType) {
        return targetType == ByteType;
    }

    @Override
    public Integer getIndex() {
        return ByteType.getValue();
    }
}
