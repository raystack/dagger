package io.odpf.dagger.core.processors.types;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * The interface Filter decorator.
 */
public interface FilterDecorator extends FilterFunction<Row>, StreamDecorator {

    @Override
    default DataStream<Row> decorate(DataStream<Row> inputStream) {
        return inputStream.filter(this);
    }

}
