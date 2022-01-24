package io.odpf.dagger.core.sink.influx.errors;

import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.metrics.Counter;

import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorStatsReporter;
import io.odpf.dagger.core.utils.Constants;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The Late record drop error.
 */
public class LateRecordDropError implements InfluxError {
    private final Counter counter;
    private static final Logger LOGGER = LoggerFactory.getLogger(LateRecordDropError.class.getName());
    private ErrorReporter errorStatsReporter;
    private static final String PREFIX = "{\"error\":\"partial write: points beyond retention policy dropped=";

    /**
     * Instantiates a new Late record drop error.
     *
     * @param initContext the context available in sink functions
     */
    public LateRecordDropError(InitContext initContext) {
        this.counter = initContext.metricGroup()
                .addGroup(Constants.SINK_INFLUX_LATE_RECORDS_DROPPED_KEY).counter("value");
        this.errorStatsReporter = new ErrorStatsReporter(initContext.metricGroup(),
                Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT);
    }

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
        return isLateDropping(throwable);
    }

    @Override
    public void handle(Iterable<Point> points, Throwable throwable) {
        reportDroppedPoints(parseDroppedPointsCount(throwable));
        errorStatsReporter.reportNonFatalException((Exception) throwable);
        logFailedPoints(points, LOGGER);
    }

    private void reportDroppedPoints(int numPoints) {
        counter.inc(numPoints);
        LOGGER.warn("Numbers of Points Dropped :" + numPoints);
    }

    private int parseDroppedPointsCount(Throwable throwable) {
        String[] split = throwable.getMessage().split("=");
        return Integer.parseInt(split[1].trim().replace("\"}", ""));
    }

    private boolean isLateDropping(Throwable throwable) {
        return throwable instanceof InfluxDBException
                && throwable.getMessage().startsWith(PREFIX);
    }
}
