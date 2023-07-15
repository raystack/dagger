package org.raystack.dagger.core.sink.influx;

import org.apache.flink.api.connector.sink.Sink.InitContext;

import org.raystack.dagger.core.sink.influx.errors.InfluxError;
import org.raystack.dagger.core.sink.influx.errors.LateRecordDropError;
import org.raystack.dagger.core.sink.influx.errors.NoError;
import org.raystack.dagger.core.sink.influx.errors.ValidError;
import org.raystack.dagger.core.sink.influx.errors.ValidException;
import org.influxdb.dto.Point;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * The Error handler for Influx sink.
 */
public class ErrorHandler implements Serializable {
    private BiConsumer<Iterable<Point>, Throwable> exceptionHandler;

    private InfluxError error;

    /**
     * Init runtime context.
     *
     * @param initContext the runtime context
     */
    public void init(InitContext initContext) {
        List<InfluxError> influxErrors = Arrays.asList(
                new LateRecordDropError(initContext),
                new ValidError(),
                new ValidException());

        exceptionHandler = (points, throwable) -> {
            error = influxErrors.stream()
                    .filter(influxError -> influxError.filterError(throwable))
                    .findFirst()
                    .orElse(new NoError());
            error.handle(points, throwable);
        };
    }
    /**
     * Gets exception handler.
     *
     * @return the exception handler
     */
    public BiConsumer<Iterable<Point>, Throwable> getExceptionHandler() {
        return exceptionHandler;
    }

    /**
     * Gets error.
     *
     * @return the error
     */
    public Optional<InfluxError> getError() {
        return Optional.ofNullable(error);
    }
}
