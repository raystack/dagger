package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * The EndOfMonth udf.
 */
public class EndOfMonth extends ScalarUdf {

    private static final Integer END_OF_DAY_HOUR = 23;
    private static final Integer END_OF_DAY_MINUTE_AND_SECOND = 59;
    private static final Integer MAX_MILLISECONDS = 999;
    private static final Integer SECOND_IN_MILLIS = 1000;

    /**
     * Calculates the seconds in Unix timestamp for end of a month of a given timestamp second and timezone.
     *
     * @param seconds  the seconds for which start of the month to be calculated
     * @param timeZone the time zone
     * @return unix timestamp in seconds for the end of the month
     * @author Rasyid
     * @team DE
     */
    public long eval(Long seconds, String timeZone) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(timeZone));
        cal.setTime(new Date(seconds * SECOND_IN_MILLIS));

        cal.set(Calendar.HOUR_OF_DAY, END_OF_DAY_HOUR);
        cal.set(Calendar.MINUTE, END_OF_DAY_MINUTE_AND_SECOND);
        cal.set(Calendar.SECOND, END_OF_DAY_MINUTE_AND_SECOND);
        cal.set(Calendar.MILLISECOND, MAX_MILLISECONDS);
        cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));

        return cal.getTimeInMillis() / SECOND_IN_MILLIS;
    }
}
