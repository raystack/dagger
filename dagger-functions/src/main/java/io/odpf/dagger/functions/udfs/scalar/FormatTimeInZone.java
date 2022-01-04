package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


/**
 * The FormatTimeInZone udf.
 */
public class FormatTimeInZone extends ScalarUdf {
    /**
     * gets Formatted time from timestamp in given timezone.
     *
     * @param timestamp  the timestamp
     * @param timeZone   the time zone eg. "Asia/Kolkata"
     * @param dateFormat time format in which we want the result
     * @return the formatted time as string
     * @author oshank
     * @team GoFood
     */
    public String eval(Timestamp timestamp, String timeZone, String dateFormat) {
        Date date = new Date(timestamp.getTime());
        DateFormat format = new SimpleDateFormat(dateFormat);
        format.setTimeZone(TimeZone.getTimeZone(timeZone));
        return format.format(date);
    }
}
