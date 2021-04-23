package io.odpf.dagger.core.sink.influx.errors;

import org.influxdb.dto.Point;

import org.slf4j.Logger;

public interface InfluxError {

    boolean hasException();

    Exception getCurrentException();

    boolean filterError(Throwable throwable);

    void handle(Iterable<Point> points, Throwable throwable);

    default void logFailedPoints(Iterable<Point> points, Logger LOGGER) {
        points.forEach(point -> LOGGER.warn("Error writing to influx {}", point.toString()));
    }
}
