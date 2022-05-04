package io.odpf.dagger.core.source.config.models;

import lombok.Getter;

import java.io.Serializable;
import java.time.Instant;

@Getter
public class TimeRange implements Serializable {
    public TimeRange(Instant startInstant, Instant endInstant) {
        this.startInstant = startInstant;
        this.endInstant = endInstant;
    }

    private Instant startInstant;

    private Instant endInstant;

    public boolean contains(Instant instant) {
        return instant.equals(startInstant) || instant.equals(endInstant) || (instant.isAfter(startInstant) && instant.isBefore(endInstant));
    }
}
