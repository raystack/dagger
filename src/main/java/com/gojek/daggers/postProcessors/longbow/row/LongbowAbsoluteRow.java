package com.gojek.daggers.postProcessors.longbow.row;

import com.gojek.daggers.postProcessors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowAbsoluteRow implements LongbowRow {
    private LongbowSchema longbowSchema;

    public LongbowAbsoluteRow(LongbowSchema longbowSchema) {
        this.longbowSchema = longbowSchema;
    }

    @Override
    public byte[] getLatest(Row input) {
        return longbowSchema.getAbsoluteKey(input, (long) longbowSchema.getValue(input, LONGBOW_LATEST));
    }

    @Override
    public byte[] getEarliest(Row input) {
        return longbowSchema.getAbsoluteKey(input, (long) longbowSchema.getValue(input, LONGBOW_EARLIEST));
    }

    @Override
    public String[] getInvalidFields() {
        return new String[]{LONGBOW_DURATION};
    }
}
