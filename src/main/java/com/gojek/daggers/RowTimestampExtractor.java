package com.gojek.daggers;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

public class RowTimestampExtractor implements AssignerWithPeriodicWatermarks<Row> {

    private int timestampRowIndex;
    private long currentTimestamp;
    private static final int MILLISECOND_FACTOR = 1000;

    public RowTimestampExtractor(int timestampRowIndex) {
        this.timestampRowIndex = timestampRowIndex;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp);
    }

    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
        Row timestampRow = (Row) element.getField(timestampRowIndex);
        long timestampSeconds = (long) timestampRow.getField(0);
        int timestampNanos = (int) timestampRow.getField(1);

        currentTimestamp = timestampSeconds * MILLISECOND_FACTOR + timestampNanos / MILLISECOND_FACTOR;
        return currentTimestamp;
    }
}
