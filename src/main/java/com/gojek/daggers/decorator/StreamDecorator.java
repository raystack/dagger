package com.gojek.daggers.decorator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.io.Serializable;

public interface StreamDecorator extends Serializable {
    Boolean canDecorate();

    DataStream<Row> decorate(DataStream<Row> inputStream);
}
