package io.odpf.dagger.core.source.config.adapter;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.odpf.dagger.core.exception.InvalidTimeRangeException;
import io.odpf.dagger.core.source.config.models.TimeRange;
import io.odpf.dagger.core.source.config.models.TimeRanges;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;

public class FileDateRangeAdaptor extends TypeAdapter<TimeRanges> {
    @Override
    public void write(JsonWriter out, TimeRanges value) {

    }

    @Override
    public TimeRanges read(JsonReader reader) throws IOException {
        TimeRanges timeRanges = new TimeRanges();
        String timeRangesString = reader.nextString();
        String[] timeRangesArray = timeRangesString.split(";");
        Arrays.asList(timeRangesArray).forEach(timeRange -> {
            String[] timestamps = timeRange.split(",");
            if (timestamps.length == 2) {
                Instant startTime = parseInstant(timestamps[0].trim());
                Instant endTime = parseInstant(timestamps[1].trim());
                if (startTime.isAfter(endTime)) {
                    throw new InvalidTimeRangeException("startTime should not be after endTime");
                }
                timeRanges.add(new TimeRange(startTime, endTime));
            } else {
                throw new InvalidTimeRangeException("The time ranges should contain two ISO format timestamps");
            }
        });
        return timeRanges;
    }

    private Instant parseInstant(String timestamp) {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .optionalStart().appendOffsetId()
                .toFormatter().withZone(ZoneOffset.UTC);
        return Instant.from(formatter.parse(timestamp));
    }
}
