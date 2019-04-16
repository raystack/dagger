package com.gojek.daggers.async.decorator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public interface MapDecorator extends MapFunction<Row, Row>, StreamDecorator {

    @Override
    default DataStream<Row> decorate(DataStream<Row> inputStream) {
        return inputStream.map(this::map);
    }

}
