package org.raystack.dagger.core.source.parquet.path;


import org.apache.flink.core.fs.Path;
import org.junit.Test;

import java.text.ParseException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class HourDatePathParserTest {

    @Test
    public void shouldReturnInstantFromFilePathWithBothHourAndDate() throws ParseException {
        Path path = new Path("gs://my-bucket/bid-log/dt=2019-10-12/hr=03/file");

        HourDatePathParser hourDatePathParser = new HourDatePathParser();

        Instant instant = hourDatePathParser.instantFromFilePath(path);

        assertEquals(1570849200, instant.getEpochSecond());
    }

    @Test
    public void shouldReturnInstantFromFilePathWithDate() throws ParseException {
        Path path = new Path("gs://my-bucket/bid-log/dt=2019-10-12/file");

        HourDatePathParser hourDatePathParser = new HourDatePathParser();

        Instant instant = hourDatePathParser.instantFromFilePath(path);

        assertEquals(1570838400, instant.getEpochSecond());
    }

    @Test
    public void shouldThrowExceptionIfFilePathNotCompatible() {
        Path path = new Path("gs://my-bucket/bid-log/date=2019-10-12/file");

        HourDatePathParser hourDatePathParser = new HourDatePathParser();

        ParseException parseException = assertThrows(ParseException.class, () -> hourDatePathParser.instantFromFilePath(path));

        assertEquals("Cannot extract timestamp from filepath for deciding order of processing.\n"
                + "File path doesn't abide with any partitioning strategy: gs://my-bucket/bid-log/date=2019-10-12/file", parseException.getMessage());
    }
}
