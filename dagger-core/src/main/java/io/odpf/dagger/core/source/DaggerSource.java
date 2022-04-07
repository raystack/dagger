package io.odpf.dagger.core.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * Interface for all types of Sources in Dagger.
 */
public interface DaggerSource {

    /**
     * Can handle boolean.
     *
     * @return boolean according to configured source
     */
    boolean canHandle();

    /**
     * @param executionEnvironment the flink execution environment
     * @param watermarkStrategy    configured watermark strategy
     * @param streamName           name of the stream
     * @return DataStream after registration
     */
    DataStreamSource register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy, String streamName);
}
