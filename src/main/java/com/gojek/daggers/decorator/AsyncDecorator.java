package com.gojek.daggers.decorator;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public abstract class AsyncDecorator implements StreamDecorator {
    public abstract Integer getAsyncIOCapacity();

    public abstract AsyncFunction getAsyncFunction();

    public abstract Integer getStreamTimeout();

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        return AsyncDataStream.orderedWait(inputStream, getAsyncFunction(), getStreamTimeout(), TimeUnit.MILLISECONDS, getAsyncIOCapacity());
    }

}
