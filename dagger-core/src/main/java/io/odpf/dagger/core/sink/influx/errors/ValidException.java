package io.odpf.dagger.core.sink.influx.errors;

import io.odpf.dagger.core.exception.InfluxWriteException;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The Valid exception.
 */
public class ValidException implements InfluxError {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidException.class.getName());
    private IOException exception;

    @Override
    public boolean hasException() {
        return true;
    }

    @Override
    public IOException getCurrentException() {
        return exception;
    }

    @Override
    public boolean filterError(Throwable throwable) {
        return throwable instanceof Exception;
    }

    @Override
    public void handle(Iterable<Point> points, Throwable throwable) {
        exception = new InfluxWriteException(throwable);
        logFailedPoints(points, LOGGER);
    }
}
