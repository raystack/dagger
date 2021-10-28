package io.odpf.dagger.common.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Duration;

public class SourceWatermark implements WatermarkStrategyDefinition {
    private boolean perPartitionWatermarkEnabled;
    private static final String ROWTIME = "rowtime";

    public SourceWatermark(boolean perPartitionWatermarkEnabled) {
        this.perPartitionWatermarkEnabled = perPartitionWatermarkEnabled;
    }

    @Override
    public WatermarkStrategy<Row> defineWaterMarkStrategy(long waterMarkDelayInMs) {
        if (perPartitionWatermarkEnabled) {
            return WatermarkStrategy.
                    <Row>forBoundedOutOfOrderness(Duration.ofMillis(waterMarkDelayInMs))
                    .withTimestampAssigner((SerializableTimestampAssigner<Row>) (element, recordTimestamp) -> {
                        int index = element.getArity() - 1;
                        return ((Timestamp) element.getField(index)).getTime();
                    });
        } else {
            return WatermarkStrategy.<Row>noWatermarks();
        }
    }
}
