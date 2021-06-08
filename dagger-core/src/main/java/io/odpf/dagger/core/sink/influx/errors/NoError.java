package io.odpf.dagger.core.sink.influx.errors;

import org.influxdb.dto.Point;

/**
 * No error found on Influx sink.
 */
public class NoError implements InfluxError {
    @Override
    public boolean hasException() {
        return false;
    }

    @Override
    public Exception getCurrentException() {
        return null;
    }

    @Override
    public boolean filterError(Throwable throwable) {
        return false;
    }

    @Override
    public void handle(Iterable<Point> points, Throwable throwable) {
    }
}
