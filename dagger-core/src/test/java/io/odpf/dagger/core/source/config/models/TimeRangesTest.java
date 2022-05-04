package io.odpf.dagger.core.source.config.models;


import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeRangesTest {

    @Test
    public void shouldReturnTrueIfInstantFallsBetweenAnyTimeRange() {
        TimeRanges timeRanges = new TimeRanges();
        timeRanges.add(new TimeRange(Instant.parse("2022-02-13T04:00:00Z"), Instant.parse("2022-02-13T07:59:00Z")));
        timeRanges.add(new TimeRange(Instant.parse("2022-02-13T14:00:00Z"), Instant.parse("2022-02-13T17:59:00Z")));
        timeRanges.add(new TimeRange(Instant.parse("2022-02-13T19:00:00Z"), Instant.parse("2022-02-13T20:59:00Z")));

        assertTrue(timeRanges.contains(Instant.parse("2022-02-13T15:00:00Z")));
    }

    @Test
    public void shouldReturnTrueIfInstantFallsBetweenAllTimeRange() {
        TimeRanges timeRanges = new TimeRanges();
        timeRanges.add(new TimeRange(Instant.parse("2022-02-13T04:00:00Z"), Instant.parse("2022-02-13T07:59:00Z")));
        timeRanges.add(new TimeRange(Instant.parse("2022-02-13T07:59:00Z"), Instant.parse("2022-02-13T08:59:00Z")));

        assertTrue(timeRanges.contains(Instant.parse("2022-02-13T07:59:00Z")));
    }

    @Test
    public void shouldReturnFalseIfInstantDoesNotFallBetweenAnyTimeRange() {
        TimeRanges timeRanges = new TimeRanges();
        timeRanges.add(new TimeRange(Instant.parse("2022-02-13T04:00:00Z"), Instant.parse("2022-02-13T07:59:00Z")));
        timeRanges.add(new TimeRange(Instant.parse("2022-02-13T14:00:00Z"), Instant.parse("2022-02-13T17:59:00Z")));
        timeRanges.add(new TimeRange(Instant.parse("2022-02-13T19:00:00Z"), Instant.parse("2022-02-13T20:59:00Z")));

        assertFalse(timeRanges.contains(Instant.parse("2022-02-13T23:00:00Z")));
    }
}
