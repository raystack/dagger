package com.gojek.daggers.longbow.row;

import com.gojek.daggers.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import static com.gojek.daggers.Constants.*;

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
