package com.gojek.daggers.postprocessors.longbow.range;

import com.gojek.daggers.postprocessors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowAbsoluteRange implements LongbowRange {
    private LongbowSchema longbowSchema;

    public LongbowAbsoluteRange(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public byte[] getUpperBound(Row input) {
        return longbowSchema.getAbsoluteKey(input, (long) longbowSchema.getValue(input, LONGBOW_LATEST));
    }

    @Override
    public byte[] getLowerBound(Row input) {
        return longbowSchema.getAbsoluteKey(input, (long) longbowSchema.getValue(input, LONGBOW_EARLIEST));
    }

    @Override
    public String[] getInvalidFields() {
        return new String[]{LONGBOW_DURATION};
    }
}
