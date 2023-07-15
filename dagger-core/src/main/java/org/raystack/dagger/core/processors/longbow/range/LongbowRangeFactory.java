package org.raystack.dagger.core.processors.longbow.range;

import org.raystack.dagger.core.exception.DaggerConfigurationException;
import org.raystack.dagger.core.utils.Constants;
import org.raystack.dagger.core.processors.longbow.LongbowSchema;

/**
 * The factor class for Longbow range.
 */
public class LongbowRangeFactory {
    /**
     * Gets longbow range.
     *
     * @param longbowSchema the longbow schema
     * @return the longbow range
     */
    public static LongbowRange getLongbowRange(LongbowSchema longbowSchema) {
        if (longbowSchema.contains(Constants.LONGBOW_DURATION_KEY)) {
            return new LongbowDurationRange(longbowSchema);
        } else if (longbowSchema.contains(Constants.LONGBOW_EARLIEST_KEY) && longbowSchema.contains(Constants.LONGBOW_LATEST_KEY)) {
            return new LongbowAbsoluteRange(longbowSchema);
        } else {
            throw new DaggerConfigurationException("Missing required field: Either (" + Constants.LONGBOW_DURATION_KEY + ") or both (" + Constants.LONGBOW_EARLIEST_KEY + " and " + Constants.LONGBOW_LATEST_KEY + ") should be passed");
        }
    }
}
