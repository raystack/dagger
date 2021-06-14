package io.odpf.dagger.core.processors.types;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * The interface Map decorator.
 */
public interface MapDecorator extends MapFunction<Row, Row>, StreamDecorator {

    @Override
    default DataStream<Row> decorate(DataStream<Row> inputStream) {
        return inputStream.map(this);
    }

}
