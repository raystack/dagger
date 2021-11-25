package io.odpf.dagger.core.sink.influx.errors;

import org.influxdb.dto.Point;

import java.io.IOException;

/**
 * No error found on Influx sink.
 */
public class NoError implements InfluxError {
    @Override
    public boolean hasException() {
        return false;
    }

    @Override
    public IOException getCurrentException() {
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
