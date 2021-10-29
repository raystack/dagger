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

    public DataStream<Row> assignTimeStampAndWatermark(DataStream<Row> inputStream, long watermarkDelayMs, boolean enablePerPartitionWatermark) {
        return !enablePerPartitionWatermark ? inputStream
                .assignTimestampsAndWatermarks(watermarkStrategyDefinition.getWatermarkStrategy(watermarkDelayMs)) : inputStream;
    }

    public DataStream<Row> assignTimeStampAndWatermark(DataStream<Row> inputStream, long watermarkDelayMs) {
        return inputStream
                .assignTimestampsAndWatermarks(watermarkStrategyDefinition.getWatermarkStrategy(watermarkDelayMs));
    }

    public FlinkKafkaConsumerBase consumerAssignTimeStampAndWatermark(FlinkKafkaConsumer<Row> flinkKafkaConsumer, long watermarkDelayMs, boolean enablePerPartitionWatermark) {
        return enablePerPartitionWatermark ? flinkKafkaConsumer
                .assignTimestampsAndWatermarks(watermarkStrategyDefinition.getWatermarkStrategy(watermarkDelayMs)) : flinkKafkaConsumer;
    }
}
