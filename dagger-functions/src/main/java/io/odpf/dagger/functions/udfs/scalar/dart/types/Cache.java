package io.odpf.dagger.functions.udfs.scalar.dart.types;

import org.apache.commons.lang3.time.DateUtils;

import java.io.Serializable;
import java.util.Date;

/**
 * The type Cache.
 */
public abstract class Cache implements Serializable {

    private Date timeOfCaching;

    /**
     * Instantiates a new Cache.
     *
     * @param timeOfCaching the time of caching
     */
    Cache(Date timeOfCaching) {
        this.timeOfCaching = timeOfCaching;
    }

    /**
     * Has expired boolean.
     *
     * @param refreshIntervalInHours the refresh interval in hours
     * @return the boolean
     */
    public boolean hasExpired(int refreshIntervalInHours) {
        if (timeOfCaching == null) {
            return true;
        }
        Date currentTime = new Date();
        Date timeOfExpire = DateUtils.addHours(timeOfCaching, refreshIntervalInHours);
        return currentTime.after(timeOfExpire);
    }

}
