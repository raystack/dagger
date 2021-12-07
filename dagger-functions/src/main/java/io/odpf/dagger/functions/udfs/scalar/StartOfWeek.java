package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * The Start of week udf.
 */
public class StartOfWeek extends ScalarUdf {

    private static final Integer FIRST_HOUR_OF_DAY = 0;
    private static final Integer DURATION_OF_DAY_IN_SECONDS = 86400;

    /**
     * Calculates the milliseconds in Unix time for start of a week of a given timestamp second and timezone.
     *
     * @param milliseconds the milliseconds for which start of the week to be calculated
     * @param timeZone     the time zone
     * @return unix timestamp in milliseconds for the start of the particular week
     * @author Rasyid
     * @team DE
     */
    public long eval(Long milliseconds, String timeZone) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(timeZone));
        cal.setTime(new Date(milliseconds));

        cal.set(Calendar.HOUR_OF_DAY, FIRST_HOUR_OF_DAY);
        cal.clear(Calendar.MINUTE);
        cal.clear(Calendar.SECOND);
        cal.clear(Calendar.MILLISECOND);
        cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() - DURATION_OF_DAY_IN_SECONDS);

        return cal.getTimeInMillis();
    }
}
