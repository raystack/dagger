package com.gojek.daggers.sink.influx.errors;

import org.influxdb.dto.Point;

public interface InfluxError {

    boolean hasException();

    Exception getCurrentException();

    boolean filterError(Throwable throwable);

    void handle(Iterable<Point> points, Throwable throwable);

}
