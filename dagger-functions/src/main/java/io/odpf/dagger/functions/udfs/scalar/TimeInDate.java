package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * The Time in date udf.
 */
public class TimeInDate extends ScalarUdf {
    private static final Integer SECOND_IN_MILLIS = 1000;

    /**
     * returns calender's time value in seconds for a given event timestamp, hour, and minute.
     *
     * @param eventTimestamp the event timestamp
     * @param hour           the hour
     * @param minute         the minute
     * @return the long
     */
    public long eval(long eventTimestamp, int hour, int minute) {
        return eval(eventTimestamp, hour, minute, "UTC");
    }

    /**
     * returns calender's time value in seconds for a given hour, minute, second and timezone.
     *
     * @param seconds  the seconds
     * @param hour     the hour
     * @param minute   the minute
     * @param timeZone the time zone
     * @return the second value
     * @author Rasyid
     * @team DE
     */
    public long eval(Long seconds, Integer hour, Integer minute, String timeZone) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone(timeZone));
        cal.setTime(new Date(seconds * SECOND_IN_MILLIS));

        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.clear(Calendar.SECOND);
        cal.clear(Calendar.MILLISECOND);

        return cal.getTimeInMillis() / SECOND_IN_MILLIS;
    }
}
