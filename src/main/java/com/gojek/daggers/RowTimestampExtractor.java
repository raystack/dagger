package com.gojek.daggers;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.sources.DefinedRowtimeAttribute;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

public class RowTimestampExtractor implements AssignerWithPeriodicWatermarks<Row>{

    private int timestampRowIndex;
    private long currentTimestamp;

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
        Row timestampRow = (Row)element.getField(timestampRowIndex);
        long timestampSeconds = (long) timestampRow.getField(0);
        int timestampNanos = (int) timestampRow.getField(1);

        currentTimestamp = timestampSeconds * 1000 + timestampNanos/1000;
        return currentTimestamp;
    }
}
