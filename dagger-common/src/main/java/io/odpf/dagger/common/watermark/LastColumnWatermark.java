package io.odpf.dagger.common.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Duration;

public class LastColumnWatermark implements WatermarkStrategyDefinition {

    @Override
    public WatermarkStrategy<Row> getWatermarkStrategy(long waterMarkDelayInMs) {
        return WatermarkStrategy.
                <Row>forBoundedOutOfOrderness(Duration.ofMillis(waterMarkDelayInMs))
                .withTimestampAssigner((SerializableTimestampAssigner<Row>) (element, recordTimestamp) -> {
                    int index = element.getArity() - 1;
                    return ((Timestamp) element.getField(index)).getTime();
                });
    }
}
