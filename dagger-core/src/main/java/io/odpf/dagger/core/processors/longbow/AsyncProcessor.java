package io.odpf.dagger.core.processors.longbow;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class AsyncProcessor {
    public DataStream<Row> orderedWait(DataStream<Row> inputStream, AsyncFunction<Row, Row> function, long timeout, TimeUnit timeunit, Integer capacity) {
        return AsyncDataStream.orderedWait(inputStream, function, timeout, timeunit, capacity);
    }
}
