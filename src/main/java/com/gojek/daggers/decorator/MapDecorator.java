package com.gojek.daggers.decorator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public abstract class MapDecorator implements MapFunction<Row, Row>, StreamDecorator {

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        return inputStream.map(this::map);
    }

}
