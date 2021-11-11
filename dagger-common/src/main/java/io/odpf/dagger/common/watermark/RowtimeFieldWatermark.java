package io.odpf.dagger.common.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;

public class RowtimeFieldWatermark implements WatermarkStrategyDefinition {
    private static final String ROWTIME = "rowtime";
    private final String[] columnNames;

    public RowtimeFieldWatermark(String[] columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public WatermarkStrategy<Row> getWatermarkStrategy(long waterMarkDelayInMs) {
        return WatermarkStrategy.
                <Row>forBoundedOutOfOrderness(Duration.ofMillis(waterMarkDelayInMs))
                .withTimestampAssigner((SerializableTimestampAssigner<Row>)
                        (element, recordTimestamp) -> ((Timestamp) element.getField(Arrays.asList(columnNames).indexOf(ROWTIME))).getTime());
    }
}
