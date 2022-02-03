package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import java.sql.Timestamp;


/**
 * The Timestamp from unix udf.
 */
public class TimestampFromUnix extends ScalarUdf {

    private static final int SECONDS_TO_MILISECONDS = 1000;

    /**
     * gets timestamp from UNIX seconds.
     *
     * @param seconds the seconds
     * @return the timestamp
     * @author asfarudin
     * @team Fraud
     */
    public Timestamp eval(Long seconds) {
        return new Timestamp(Math.abs(seconds) * SECONDS_TO_MILISECONDS);
    }
}
