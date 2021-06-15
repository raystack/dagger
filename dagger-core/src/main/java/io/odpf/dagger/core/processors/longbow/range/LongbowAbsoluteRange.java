package io.odpf.dagger.core.processors.longbow.range;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import io.odpf.dagger.core.utils.Constants;

import org.apache.flink.types.Row;

/**
 * Absolute range on Longbow.
 */
public class LongbowAbsoluteRange implements LongbowRange {
    private LongbowSchema longbowSchema;

    /**
     * Instantiates a new Longbow absolute range.
     *
     * @param longbowSchema the longbow schema
     */
    public LongbowAbsoluteRange(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public byte[] getUpperBound(Row input) {
        return longbowSchema.getAbsoluteKey(input, (long) longbowSchema.getValue(input, Constants.LONGBOW_LATEST_KEY));
    }

    @Override
    public byte[] getLowerBound(Row input) {
        return longbowSchema.getAbsoluteKey(input, (long) longbowSchema.getValue(input, Constants.LONGBOW_EARLIEST_KEY));
    }

    @Override
    public String[] getInvalidFields() {
        return new String[]{Constants.LONGBOW_DURATION_KEY};
    }
}
