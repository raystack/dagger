package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * The EndOfWeek udf.
 */
public class EndOfWeek extends ScalarUdf {

    private static final Integer END_OF_DAY_HOUR = 23;
    private static final Integer END_OF_DAY_MINUTE_AND_SECOND = 59;
    private static final Integer DAY_SPAN = 7;
    private static final Integer MAX_MILLISECONDS = 999;

    /**
     * Calculates the milliSeconds in Unix time for end of a week of a given timestamp second and timezone.
     *
     * @param milliSeconds the milliSeconds for which start of the week to be calculated
     * @param timeZone     the time zone
     * @return unix timestamp in milliSeconds for the end of the week
     * @author Rasyid
     * @team DE
     */
    public long eval(Long milliSeconds, String timeZone) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(timeZone));
        cal.setTime(new Date(milliSeconds));

        cal.set(Calendar.HOUR_OF_DAY, END_OF_DAY_HOUR);
        cal.set(Calendar.MINUTE, END_OF_DAY_MINUTE_AND_SECOND);
        cal.set(Calendar.SECOND, END_OF_DAY_MINUTE_AND_SECOND);
        cal.set(Calendar.MILLISECOND, MAX_MILLISECONDS);
        cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
        cal.add(Calendar.DATE, DAY_SPAN);

        return cal.getTimeInMillis();
    }
}
