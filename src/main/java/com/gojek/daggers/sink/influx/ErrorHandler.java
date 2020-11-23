package com.gojek.daggers.sink.influx;

import org.apache.flink.api.common.functions.RuntimeContext;

import com.gojek.daggers.sink.influx.errors.DroppedRecord;
import com.gojek.daggers.sink.influx.errors.InfluxError;
import com.gojek.daggers.sink.influx.errors.NoError;
import com.gojek.daggers.sink.influx.errors.ValidError;
import com.gojek.daggers.sink.influx.errors.ValidException;
import org.influxdb.dto.Point;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

public class ErrorHandler implements Serializable {
    private BiConsumer<Iterable<Point>, Throwable> exceptionHandler;

    private InfluxError error;

    public void init(RuntimeContext runtimeContext) {
        List<InfluxError> influxErrors = Arrays.asList(
                new DroppedRecord(runtimeContext),
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

    public BiConsumer<Iterable<Point>, Throwable> getExceptionHandler() {
        return exceptionHandler;
    }

    public Optional<InfluxError> getError() {
        return Optional.ofNullable(error);
    }
}
