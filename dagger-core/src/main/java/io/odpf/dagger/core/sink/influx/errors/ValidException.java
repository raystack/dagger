package io.odpf.dagger.core.sink.influx.errors;

import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Valid exception.
 */
public class ValidException implements InfluxError {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidException.class.getName());
    private Exception exception;

    @Override
    public boolean hasException() {
        return true;
    }

    @Override
    public Exception getCurrentException() {
        return exception;
    }

    @Override
    public boolean filterError(Throwable throwable) {
        return throwable instanceof Exception;
    }

    @Override
    public void handle(Iterable<Point> points, Throwable throwable) {
        exception = (Exception) throwable;
        logFailedPoints(points, LOGGER);
    }
}
