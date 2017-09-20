package com.gojek.daggers;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.time.Instant;

public class RowTimestampExtractor implements AssignerWithPeriodicWatermarks<Row> {

    private int timestampRowIndex;
    private int watermarkDelay;
    private long currentTimestamp;

    public RowTimestampExtractor(int timestampRowIndex, Configuration config) {
        this.timestampRowIndex = timestampRowIndex;
        this.watermarkDelay = config.getInteger("WATERMARK_DELAY_MS", 0);
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp - this.watermarkDelay);
    }

    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
        Row timestampRow = (Row) element.getField(timestampRowIndex);
        long timestampSeconds = (long) timestampRow.getField(0);
        long timestampNanos = (int) timestampRow.getField(1);

        currentTimestamp = Instant.ofEpochSecond(timestampSeconds, timestampNanos).toEpochMilli();
        return currentTimestamp;
    }
}
