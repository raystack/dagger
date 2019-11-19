package com.gojek.daggers.postProcessors.longbow.row;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import static com.gojek.daggers.utils.Constants.LONGBOW_EARLIEST;
import static com.gojek.daggers.utils.Constants.LONGBOW_LATEST;

public class LongbowDurationRow implements LongbowRow {
    private LongbowSchema longbowSchema;

    public LongbowDurationRow(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public byte[] getLatest(Row input) {
        return longbowSchema.getKey(input, 0);
    }

    @Override
    public byte[] getEarliest(Row input) {
        return longbowSchema.getKey(input, longbowSchema.getDurationInMillis(input));
    }

    @Override
    public String[] getInvalidFields() {
        return new String[]{LONGBOW_EARLIEST, LONGBOW_LATEST};
    }
}
