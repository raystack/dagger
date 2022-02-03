package io.odpf.dagger.core.sink.influx.errors;

import org.influxdb.dto.Point;

import org.slf4j.Logger;

import java.io.IOException;

/**
 * The interface Influx error.
 */
public interface InfluxError {

    /**
     * Check if it has exception.
     *
     * @return the boolean
     */
    boolean hasException();

    /**
     * Gets current exception.
     *
     * @return the current exception
     */
    IOException getCurrentException();

    /**
     * Filter the error.
     *
     * @param throwable the throwable
     * @return the boolean
     */
    boolean filterError(Throwable throwable);

    /**
     * Handle the error.
     *
     * @param points    the points
     * @param throwable the throwable
     */
    void handle(Iterable<Point> points, Throwable throwable);

    /**
     * Log failed points.
     *
     * @param points the points
     * @param logger the logger
     */
    default void logFailedPoints(Iterable<Point> points, Logger logger) {
        points.forEach(point -> logger.warn("Error writing to influx {}", point.toString()));
    }
}
