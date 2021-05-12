package io.odpf.dagger.functions.udfs.scalar.dart.types;

import org.apache.commons.lang3.time.DateUtils;

import java.io.Serializable;
import java.util.Date;

public abstract class Cache implements Serializable {

    private Date timeOfCaching;

    Cache(Date timeOfCaching) {
        this.timeOfCaching = timeOfCaching;
    }

    public boolean hasExpired(int refreshIntervalInHours) {
        if (timeOfCaching == null) {
            return true;
        }
        Date currentTime = new Date();
        Date timeOfExpire = DateUtils.addHours(currentTime, refreshIntervalInHours);
        return timeOfCaching.after(timeOfExpire);
    }

}
