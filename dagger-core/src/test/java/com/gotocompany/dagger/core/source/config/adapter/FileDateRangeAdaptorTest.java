package com.gotocompany.dagger.core.source.config.adapter;


import com.google.gson.stream.JsonReader;
import com.gotocompany.dagger.core.exception.InvalidTimeRangeException;
import com.gotocompany.dagger.core.source.config.models.TimeRange;
import com.gotocompany.dagger.core.source.config.models.TimeRangePool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class FileDateRangeAdaptorTest {

    @Mock
    private JsonReader jsonReader;

    @Before
    public void setup() {
        initMocks(this);
    }


    @Test
    public void shouldParseSingleDateRange() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00,2022-02-13T17:59:00");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRangePool timeRangePool = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRangePool.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldParseSingleDateRangeWithUTCTimezone() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00Z,2022-02-13T17:59:00Z");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRangePool timeRangePool = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRangePool.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldParseSingleDateRangeWithExtraSpaces() throws IOException {
        when(jsonReader.nextString()).thenReturn("  2022-02-13T14:00:00 ,  2022-02-13T17:59:00  ");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRangePool timeRangePool = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRangePool.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldThrowExceptionIfBothTimestampsNotGiven() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        InvalidTimeRangeException invalidTimeRangeException = assertThrows(InvalidTimeRangeException.class, () -> fileDateRangeAdaptor.read(jsonReader));

        assertEquals("Each time range should contain a pair of ISO format timestamps separated by comma. Multiple ranges can be provided separated by ;", invalidTimeRangeException.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfStartTimeIsAfterEndTime() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T17:59:00Z,2022-02-13T14:00:00Z");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        InvalidTimeRangeException invalidTimeRangeException = assertThrows(InvalidTimeRangeException.class, () -> fileDateRangeAdaptor.read(jsonReader));

        assertEquals("startTime should not be after endTime", invalidTimeRangeException.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfTimestampsNotGivenInISOFormat() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13/14:00:00,2022-02-13/17:59:00");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        InvalidTimeRangeException invalidTimeRangeException = assertThrows(InvalidTimeRangeException.class, () -> fileDateRangeAdaptor.read(jsonReader));

        assertEquals("Unable to parse timestamp: 2022-02-13/14:00:00 with supported date formats i.e. yyyy-MM-ddTHH:mm:ssZ and yyyy-MM-ddTHH:mm:ss", invalidTimeRangeException.getMessage());
    }

    @Test
    public void shouldParseMultipleDateRanges() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00,2022-02-13T17:59:00;2022-02-14T15:30:00,2022-02-14T17:35:00");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRangePool timeRangePool = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRangePool.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
        assertEquals(1644852600, timeRangesList.get(1).getStartInstant().getEpochSecond());
        assertEquals(1644860100, timeRangesList.get(1).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldParseMultipleDateRangesWithExtraSpaces() throws IOException {
        when(jsonReader.nextString()).thenReturn(" 2022-02-13T14:00:00 , 2022-02-13T17:59:00 ;  2022-02-14T15:30:00 ,2022-02-14T17:35:00 ");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRangePool timeRangePool = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRangePool.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
        assertEquals(1644852600, timeRangesList.get(1).getStartInstant().getEpochSecond());
        assertEquals(1644860100, timeRangesList.get(1).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldThrowExceptionIfTimestampsGivenWithOtherTimezoneOffset() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00+05:30,2022-02-13T17:59:00+05:30");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        InvalidTimeRangeException invalidTimeRangeException = assertThrows(InvalidTimeRangeException.class, () -> fileDateRangeAdaptor.read(jsonReader));

        assertEquals("Unable to parse timestamp: 2022-02-13T14:00:00+05:30 with supported date formats i.e. yyyy-MM-ddTHH:mm:ssZ and yyyy-MM-ddTHH:mm:ss", invalidTimeRangeException.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfTimestampRangesAreValidButWithInvalidDelimeter() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00,2022-02-13T17:59:00|2022-02-14T15:30:00,2022-02-14T17:35:00");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        InvalidTimeRangeException invalidTimeRangeException = assertThrows(InvalidTimeRangeException.class, () -> fileDateRangeAdaptor.read(jsonReader));

        assertEquals("Each time range should contain a pair of ISO format timestamps separated by comma. Multiple ranges can be provided separated by ;", invalidTimeRangeException.getMessage());
    }

}
