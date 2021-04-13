package io.odpf.dagger.processors.longbow.range;

import io.odpf.dagger.processors.longbow.LongbowSchema;
import io.odpf.dagger.utils.Constants;

import org.apache.flink.types.Row;

public class LongbowAbsoluteRange implements LongbowRange {
    private LongbowSchema longbowSchema;

    public LongbowAbsoluteRange(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public byte[] getUpperBound(Row input) {
        return longbowSchema.getAbsoluteKey(input, (long) longbowSchema.getValue(input, Constants.LONGBOW_LATEST));
    }

    @Override
    public byte[] getLowerBound(Row input) {
        return longbowSchema.getAbsoluteKey(input, (long) longbowSchema.getValue(input, Constants.LONGBOW_EARLIEST));
    }

    @Override
    public String[] getInvalidFields() {
        return new String[]{Constants.LONGBOW_DURATION};
    }
}
