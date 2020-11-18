package com.gojek.daggers.sink.influx.errors;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

import com.gojek.daggers.utils.Constants;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DroppedRecord implements InfluxError {
    private final Counter counter;
    private static final Logger LOGGER = LoggerFactory.getLogger(DroppedRecord.class.getName());
    static String PREFIX = "{\"error\":\"partial write: points beyond retention policy dropped=";

    public DroppedRecord(RuntimeContext runtimeContext) {
        this.counter = runtimeContext.getMetricGroup()
                .addGroup(Constants.INFLUX_LATE_RECORDS_DROPPED_KEY).counter("value");
    }

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
        return isLateDropping(throwable);
    }

    @Override
    public void handle(Iterable<Point> points, Throwable throwable) {
        reportDroppedPoints(parseDroppedPointsCount(throwable));
        logFailedPoints(points, LOGGER);
    }

    private void reportDroppedPoints(int numPoints) {
        counter.inc(numPoints);
        LOGGER.warn("Numbers of Points Dropped :" + numPoints);
    }

    private int parseDroppedPointsCount(Throwable throwable) {
        String[] split = throwable.getMessage().split("=");
        return Integer.parseInt(split[1].replace("\"}\n", ""));
    }

    private boolean isLateDropping(Throwable throwable) {
        return throwable instanceof InfluxDBException &&
                throwable.getMessage().startsWith(PREFIX);
    }
}
