package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * The Start of month udf.
 */
public class StartOfMonth extends ScalarUdf {

    private static final Integer FIRST_HOUR_OF_DAY = 0;
    private static final Integer DURATION_OF_DAY_IN_SECONDS = 86400;
    private static final Integer SECOND_IN_MILLIS = 1000;

    /**
     * Calculates the seconds in Unix time for start of a month of a given timestamp second and timezone.
     *
     * @param seconds  the seconds for which start of the month to be calculated
     * @param timeZone the time zone
     * @return unix timestamp in seconds for the start of the month
     * @author Rasyid
     * @team DE
     */
    public long eval(Long seconds, String timeZone) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(timeZone));
        cal.setTime(new Date(seconds * SECOND_IN_MILLIS));

        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.HOUR_OF_DAY, FIRST_HOUR_OF_DAY);
        cal.clear(Calendar.MINUTE);
        cal.clear(Calendar.SECOND);
        cal.clear(Calendar.MILLISECOND);
        cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() - DURATION_OF_DAY_IN_SECONDS);

        return cal.getTimeInMillis() / SECOND_IN_MILLIS;
    }
}
