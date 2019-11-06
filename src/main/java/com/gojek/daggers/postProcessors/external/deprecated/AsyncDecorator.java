package com.gojek.daggers.postProcessors.external.deprecated;

import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public interface AsyncDecorator extends StreamDecorator {

    Integer getAsyncIOCapacity();

    AsyncFunction getAsyncFunction();

    Integer getStreamTimeout();

    @Override
    default DataStream<Row> decorate(DataStream<Row> inputStream) {
        return AsyncDataStream.orderedWait(inputStream, getAsyncFunction(), getStreamTimeout(), TimeUnit.MILLISECONDS, getAsyncIOCapacity());
    }
}
