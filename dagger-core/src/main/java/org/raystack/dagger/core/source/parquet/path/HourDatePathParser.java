package org.raystack.dagger.core.source.parquet.path;

import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HourDatePathParser implements PathParser, Serializable {
    @Override
    public Instant instantFromFilePath(Path path) throws ParseException {
        Pattern filePathPattern = Pattern.compile("^.*/dt=([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])/(hr=([0-9][0-9]))?.*$");
        Matcher matcher = filePathPattern.matcher(path.toString());
        final int hourMatcherGroupNumber = 3;
        final int dateMatcherGroupNumber = 1;
        boolean matchFound = matcher.find();
        if (matchFound && matcher.group(hourMatcherGroupNumber) != null && matcher.group(dateMatcherGroupNumber) != null) {
            return convertToInstant(matcher.group(dateMatcherGroupNumber), matcher.group(hourMatcherGroupNumber));
        } else if (matchFound && matcher.group(hourMatcherGroupNumber) == null && matcher.group(dateMatcherGroupNumber) != null) {
            return convertToInstant(matcher.group(dateMatcherGroupNumber));
        } else {
            String message = String.format("Cannot extract timestamp from filepath for deciding order of processing.\n"
                    + "File path doesn't abide with any partitioning strategy: %s", path);
            throw new ParseException(message, 0);
        }
    }

    private Instant convertToInstant(String dateSegment) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        return simpleDateFormat.parse(dateSegment).toInstant();
    }

    private Instant convertToInstant(String dateSegment, String hourSegment) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        String dateHourString = String.join(" ", dateSegment, hourSegment);
        return simpleDateFormat.parse(dateHourString).toInstant();
    }
}
