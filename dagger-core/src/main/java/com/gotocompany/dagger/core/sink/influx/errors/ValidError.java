package com.gotocompany.dagger.core.sink.influx.errors;

import com.gotocompany.dagger.core.exception.InfluxWriteException;
import com.gotocompany.dagger.core.sink.influx.InfluxDBSink;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The Valid error.
 */
public class ValidError implements InfluxError {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBSink.class.getName());
    private IOException error;

    @Override
    public boolean hasException() {
        return true;
    }

    @Override
    public IOException getCurrentException() {
        return error;
    }

    @Override
    public boolean filterError(Throwable throwable) {
        return throwable instanceof Error;
    }

    @Override
    public void handle(Iterable<Point> points, Throwable throwable) {
        error = new InfluxWriteException(throwable);
        logFailedPoints(points, LOGGER);
    }
}
