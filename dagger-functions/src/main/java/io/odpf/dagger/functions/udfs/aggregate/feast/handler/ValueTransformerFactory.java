package io.odpf.dagger.functions.udfs.aggregate.feast.handler;

import java.util.Arrays;
import java.util.List;

/**
 * The factory class for Value transformer.
 */
public class ValueTransformerFactory {
    /**
     * Gets value transformers.
     *
     * @return the value transformers
     */
    public static List<ValueTransformer> getValueTransformers() {
        return Arrays.asList(new BooleanValueTransformer(),
                new ByteValueTransformer(),
                new DoubleValueTransformer(),
                new FloatValueTransformer(),
                new IntegerValueTransformer(),
                new LongValueTransformer(),
                new StringValueTransformer(),
                new TimestampValueTransformer(),
                new NullValueTransformer(),
                new BigDecimalValueTransformer());
    }
}
