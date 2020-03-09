package com.gojek.daggers.postProcessors.longbow.range;

import com.gojek.daggers.exception.DaggerConfigurationException;
import com.gojek.daggers.postProcessors.longbow.LongbowSchema;

import static com.gojek.daggers.utils.Constants.*;

public class LongbowRangeFactory {
    public static LongbowRange getLongbowRange(LongbowSchema longbowSchema) {
        if (longbowSchema.contains(LONGBOW_DURATION))
            return new LongbowDurationRange(longbowSchema);
        else if (longbowSchema.contains(LONGBOW_EARLIEST) && longbowSchema.contains(LONGBOW_LATEST))
            return new LongbowAbsoluteRange(longbowSchema);
        else
            throw new DaggerConfigurationException("Missing required field: Either (" + LONGBOW_DURATION + ") or both (" + LONGBOW_EARLIEST + " and " + LONGBOW_LATEST + ") should be passed");
    }
}
