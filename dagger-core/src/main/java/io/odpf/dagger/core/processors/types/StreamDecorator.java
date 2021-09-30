package io.odpf.dagger.core.processors.types;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * The interface Stream decorator.
 */
public interface StreamDecorator extends Serializable {
    /**
     * Check if can decorate the Stream or not.
     *
     * @return the boolean
     */
    Boolean canDecorate();

    /**
     * Decorate data stream.
     *
     * @param inputStream the input stream
     * @return the data stream
     */
    DataStream<Row> decorate(DataStream<Row> inputStream);
}
