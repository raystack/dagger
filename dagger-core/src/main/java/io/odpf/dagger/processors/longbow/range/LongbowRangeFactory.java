package io.odpf.dagger.processors.longbow.range;

import io.odpf.dagger.exception.DaggerConfigurationException;
import io.odpf.dagger.processors.longbow.LongbowSchema;
import io.odpf.dagger.utils.Constants;

public class LongbowRangeFactory {
    public static LongbowRange getLongbowRange(LongbowSchema longbowSchema) {
        if (longbowSchema.contains(Constants.LONGBOW_DURATION))
            return new LongbowDurationRange(longbowSchema);
        else if (longbowSchema.contains(Constants.LONGBOW_EARLIEST) && longbowSchema.contains(Constants.LONGBOW_LATEST))
            return new LongbowAbsoluteRange(longbowSchema);
        else
            throw new DaggerConfigurationException("Missing required field: Either (" + Constants.LONGBOW_DURATION + ") or both (" + Constants.LONGBOW_EARLIEST + " and " + Constants.LONGBOW_LATEST + ") should be passed");
    }
}
