package com.gojek.daggers.sink.influx;

import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.BiConsumer;

public class InfluxErrorHandler implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxRowSink.class.getName());

    private Exception error = null;

    private BiConsumer<Iterable<Point>, Throwable> exceptionHandler = null;

    public void init() {
        exceptionHandler = (points, throwable) -> {
            if (throwable instanceof Exception) {
                error = (Exception) throwable;
            } else {
                error = new Exception(throwable);
            }
            points.forEach(point -> LOGGER.error("Error writing to influx {}", point.toString()));
        };
    }

    public BiConsumer<Iterable<Point>, Throwable> getExceptionHandler() {
        return exceptionHandler;
    }

    public boolean hasError() {
        return error != null;
    }

    public Exception getError() {
        return error;
    }
}
