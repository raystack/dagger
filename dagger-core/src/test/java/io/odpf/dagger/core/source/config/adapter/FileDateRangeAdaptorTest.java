package io.odpf.dagger.core.source.config.adapter;


import com.google.gson.stream.JsonReader;
import io.odpf.dagger.core.exception.InvalidTimeRangeException;
import io.odpf.dagger.core.source.config.models.TimeRange;
import io.odpf.dagger.core.source.config.models.TimeRanges;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.time.format.DateTimeParseException;
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

        TimeRanges timeRanges = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRanges.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldParseSingleDateRangeWithUTCTimezone() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00Z,2022-02-13T17:59:00Z");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRanges timeRanges = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRanges.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldParseSingleDateRangeWithExtraSpaces() throws IOException {
        when(jsonReader.nextString()).thenReturn("  2022-02-13T14:00:00 ,  2022-02-13T17:59:00  ");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRanges timeRanges = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRanges.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldThrowExceptionIfBothTimestampsNotGiven() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        InvalidTimeRangeException invalidTimeRangeException = assertThrows(InvalidTimeRangeException.class, () -> fileDateRangeAdaptor.read(jsonReader));

        assertEquals("The time ranges should contain two ISO format timestamps", invalidTimeRangeException.getMessage());
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

        DateTimeParseException dateTimeParseException = assertThrows(DateTimeParseException.class, () -> fileDateRangeAdaptor.read(jsonReader));

        assertEquals("Text '2022-02-13/14:00:00' could not be parsed at index 10", dateTimeParseException.getMessage());
    }

    @Test
    public void shouldParseMultipleDateRanges() throws IOException {
        when(jsonReader.nextString()).thenReturn("2022-02-13T14:00:00,2022-02-13T17:59:00;2022-02-14T15:30:00,2022-02-14T17:35:00");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRanges timeRanges = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRanges.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
        assertEquals(1644852600, timeRangesList.get(1).getStartInstant().getEpochSecond());
        assertEquals(1644860100, timeRangesList.get(1).getEndInstant().getEpochSecond());
    }

    @Test
    public void shouldParseMultipleDateRangesWithExtraSpaces() throws IOException {
        when(jsonReader.nextString()).thenReturn(" 2022-02-13T14:00:00 , 2022-02-13T17:59:00 ;  2022-02-14T15:30:00 ,2022-02-14T17:35:00 ");
        FileDateRangeAdaptor fileDateRangeAdaptor = new FileDateRangeAdaptor();

        TimeRanges timeRanges = fileDateRangeAdaptor.read(jsonReader);
        List<TimeRange> timeRangesList = timeRanges.getTimeRanges();

        assertEquals(1644760800, timeRangesList.get(0).getStartInstant().getEpochSecond());
        assertEquals(1644775140, timeRangesList.get(0).getEndInstant().getEpochSecond());
        assertEquals(1644852600, timeRangesList.get(1).getStartInstant().getEpochSecond());
        assertEquals(1644860100, timeRangesList.get(1).getEndInstant().getEpochSecond());
    }

}
