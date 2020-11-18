package com.gojek.daggers.sink.influx.errors;

import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        points.forEach(point -> LOGGER.error("Error writing to influx {}", point.toString()));
    }
}
