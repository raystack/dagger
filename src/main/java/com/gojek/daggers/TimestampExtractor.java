package com.gojek.daggers;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

public class TimestampExtractor implements AssignerWithPeriodicWatermarks<Row>{

    private int timestampRowIndex;
    private long currentTimestamp;

    public TimestampExtractor(int timestampRowIndex) {

        this.timestampRowIndex = timestampRowIndex;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp);
    }

    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
        Row timestampRow = (Row)element.getField(timestampRowIndex);
        long timestampSeconds = (long) timestampRow.getField(0);
        int timestampNanos = (int) timestampRow.getField(1);

        currentTimestamp = timestampSeconds + timestampNanos / (1000 * 1000);
        return currentTimestamp;
    }
}
