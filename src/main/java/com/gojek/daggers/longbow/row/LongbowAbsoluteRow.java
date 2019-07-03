package com.gojek.daggers.longbow.row;

import com.gojek.daggers.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import static com.gojek.daggers.Constants.*;

public class LongbowAbsoluteRow implements LongbowRow {
    private LongbowSchema longbowSchema;

    public LongbowAbsoluteRow(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public byte[] getLatest(Row input) {
        return longbowSchema.getAbsoluteKey(input, Long.parseLong((String) longbowSchema.getValue(input, LONGBOW_LATEST)));
    }

    @Override
    public byte[] getEarliest(Row input) {
        return longbowSchema.getAbsoluteKey(input, Long.parseLong((String) longbowSchema.getValue(input, LONGBOW_EARLIEST)));
    }

    @Override
    public String[] getInvalidFields() {
        return new String[]{LONGBOW_DURATION};
    }
}
