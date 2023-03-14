package com.gotocompany.dagger.core.source.config.models;


import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeRangeTest {

    @Test
    public void shouldReturnTrueIfInstantIsBetweenStartAndEndTime() {
        TimeRange timeRange = new TimeRange(Instant.parse("2022-02-13T14:00:00Z"), Instant.parse("2022-02-13T17:59:00Z"));
        assertTrue(timeRange.contains(Instant.parse("2022-02-13T15:00:00Z")));
    }

    @Test
    public void shouldReturnTrueIfInstantIsEqualToStartTime() {
        TimeRange timeRange = new TimeRange(Instant.parse("2022-02-13T14:00:00Z"), Instant.parse("2022-02-13T17:59:00Z"));
        assertTrue(timeRange.contains(Instant.parse("2022-02-13T14:00:00Z")));
    }

    @Test
    public void shouldReturnTrueIfInstantIsEqualToEndTime() {
        TimeRange timeRange = new TimeRange(Instant.parse("2022-02-13T14:00:00Z"), Instant.parse("2022-02-13T17:59:00Z"));
        assertTrue(timeRange.contains(Instant.parse("2022-02-13T17:59:00Z")));
    }


    @Test
    public void shouldReturnFalseIfInstantIsBeforeStartTime() {
        TimeRange timeRange = new TimeRange(Instant.parse("2022-02-13T14:00:00Z"), Instant.parse("2022-02-13T17:59:00Z"));
        assertFalse(timeRange.contains(Instant.parse("2022-02-13T12:00:00Z")));
    }

    @Test
    public void shouldReturnFalseIfInstantIsAfterEndTime() {
        TimeRange timeRange = new TimeRange(Instant.parse("2022-02-13T14:00:00Z"), Instant.parse("2022-02-13T17:59:00Z"));
        assertFalse(timeRange.contains(Instant.parse("2022-02-13T19:00:00Z")));
    }

}
