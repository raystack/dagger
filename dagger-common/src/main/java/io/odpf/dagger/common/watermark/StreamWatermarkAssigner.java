package io.odpf.dagger.common.watermark;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class StreamWatermarkAssigner implements Serializable {
    private WatermarkStrategyDefinition watermarkStrategyDefinition;

    public StreamWatermarkAssigner(WatermarkStrategyDefinition watermarkStrategyDefinition) {
        this.watermarkStrategyDefinition = watermarkStrategyDefinition;
    }

    public DataStream<Row> assignTimeStampAndWatermark(DataStream<Row> inputStream, long watermarkDelayMs) {
        return inputStream
                .assignTimestampsAndWatermarks(watermarkStrategyDefinition.defineWaterMarkStrategy(watermarkDelayMs));
    }

    public FlinkKafkaConsumerBase assignTimeStampAndWatermark(FlinkKafkaConsumer<Row> flinkKafkaConsumer, long watermarkDelayMs) {
        return flinkKafkaConsumer
                .assignTimestampsAndWatermarks(watermarkStrategyDefinition.defineWaterMarkStrategy(watermarkDelayMs));
    }
}
