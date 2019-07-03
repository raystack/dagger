package com.gojek.daggers.longbow.row;

import com.gojek.daggers.DaggerConfigurationException;
import com.gojek.daggers.longbow.LongbowSchema;

import static com.gojek.daggers.Constants.*;

public class LongbowRowFactory {
    public static LongbowRow getLongbowRow(LongbowSchema longbowSchema) {
        if (longbowSchema.contains(LONGBOW_DURATION))
            return new LongbowDurationRow(longbowSchema);
        else if (longbowSchema.contains(LONGBOW_EARLIEST) && longbowSchema.contains(LONGBOW_LATEST))
            return new LongbowAbsoluteRow(longbowSchema);
        else
            throw new DaggerConfigurationException("Missing required field: Either (" + LONGBOW_DURATION + ") or both (" + LONGBOW_EARLIEST + " and " + LONGBOW_LATEST + ") should be passed");
    }
}
