package io.odpf.dagger.core.source.config.adapter;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.odpf.dagger.core.exception.InvalidTimeRangeException;
import io.odpf.dagger.core.source.config.models.TimeRange;
import io.odpf.dagger.core.source.config.models.TimeRangePool;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

public class FileDateRangeAdaptor extends TypeAdapter<TimeRangePool> {
    @Override
    public void write(JsonWriter out, TimeRangePool value) {

    }

    @Override
    public TimeRangePool read(JsonReader reader) throws IOException {
        TimeRangePool timeRangePool = new TimeRangePool();
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
                timeRangePool.add(new TimeRange(startTime, endTime));
            } else {
                throw new InvalidTimeRangeException("Each time range should contain a pair of ISO format timestamps separated by comma. Multiple ranges can be provided separated by ;");
            }
        });
        return timeRangePool;
    }

    private Instant parseInstant(String timestamp) {
        String utcDateFormatPattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat utcDateFormat = new SimpleDateFormat(utcDateFormatPattern);
        utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        String localDataFormatPattern = "yyyy-MM-dd'T'HH:mm:ss";
        SimpleDateFormat localDateFormat = new SimpleDateFormat(localDataFormatPattern);
        localDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Date dateTime;
        if (timestamp.length() == utcDateFormatPattern.replace("'", "").length()) {
            dateTime = parse(timestamp, utcDateFormat);
        } else if (timestamp.length() == localDataFormatPattern.replace("'", "").length()) {
            dateTime = parse(timestamp, localDateFormat);
        } else {
            throw new InvalidTimeRangeException(String.format("Unable to parse timestamp: %s with supported date formats i.e. yyyy-MM-dd'T'HH:mm:ssZ and yyyy-MM-dd'T'HH:mm:ss", timestamp));
        }

        return dateTime.toInstant();
    }

    private Date parse(String timestamp, SimpleDateFormat simpleDateFormat) {
        try {
            return simpleDateFormat.parse(timestamp);
        } catch (ParseException e) {
            throw new InvalidTimeRangeException(String.format("Unable to parse timestamp: %s with supported date formats i.e. yyyy-MM-dd'T'HH:mm:ssZ and yyyy-MM-dd'T'HH:mm:ss", timestamp));
        }
    }
}
