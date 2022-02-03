package io.odpf.dagger.common.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.types.Row;

import java.io.Serializable;

public interface WatermarkStrategyDefinition extends Serializable {
    WatermarkStrategy<Row> getWatermarkStrategy(long waterMarkDelayInMs);
}
