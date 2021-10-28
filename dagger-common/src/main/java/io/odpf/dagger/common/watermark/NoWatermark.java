package io.odpf.dagger.common.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.types.Row;

public class NoWatermark implements WatermarkStrategyDefinition {
    @Override
    public WatermarkStrategy<Row> getWatermark(long waterMarkDelayInMs) {
        return WatermarkStrategy.noWatermarks();
    }
}
