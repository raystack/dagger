package io.odpf.dagger.core.source.config.models;

import lombok.Getter;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TimeRanges implements Serializable {
    public TimeRanges() {
        this.timeRanges = new ArrayList<>();
    }

    @Getter
    private List<TimeRange> timeRanges;

    public boolean add(TimeRange timeRange) {
        return timeRanges.add(timeRange);
    }

    public boolean contains(Instant instant) {
        return timeRanges.stream().anyMatch(timeRange -> timeRange.contains(instant));
    }
}
