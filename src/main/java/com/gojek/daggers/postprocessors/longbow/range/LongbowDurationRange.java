package com.gojek.daggers.postprocessors.longbow.range;

import com.gojek.daggers.postprocessors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import static com.gojek.daggers.utils.Constants.LONGBOW_EARLIEST;
import static com.gojek.daggers.utils.Constants.LONGBOW_LATEST;

public class LongbowDurationRange implements LongbowRange {
    private LongbowSchema longbowSchema;

    public LongbowDurationRange(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public byte[] getUpperBound(Row input) {
        return longbowSchema.getKey(input, 0);
    }

    @Override
    public byte[] getLowerBound(Row input) {
        return longbowSchema.getKey(input, longbowSchema.getDurationInMillis(input));
    }

    @Override
    public String[] getInvalidFields() {
        return new String[]{LONGBOW_EARLIEST, LONGBOW_LATEST};
    }
}
