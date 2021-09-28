package io.odpf.dagger.core.processors.longbow;

import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.exception.InvalidLongbowDurationException;
import io.odpf.dagger.core.processors.longbow.validator.LongbowType;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Timestamp;
import java.util.Arrays;

import static org.junit.Assert.*;

public class LongbowSchemaTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private LongbowSchema longBowSchema;
    private Row defaultRow;
    private Long defaultTimestampInMillis;
    private Timestamp defaultTimestamp;

    @Before
    public void setup() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration"};
        longBowSchema = new LongbowSchema(columnNames);
        defaultTimestampInMillis = 433321L;
        defaultTimestamp = new Timestamp(defaultTimestampInMillis);
        defaultRow = Row.of("driver1", "order1", defaultTimestamp, "24h");
    }

    @Test
    public void shouldReturnRowKeyForGivenInputWhenOffsetIsZero() {
        byte[] rowKey = longBowSchema.getKey(defaultRow, 0);

        long expectedTimestamp = Long.MAX_VALUE - defaultTimestampInMillis;
        assertEquals("driver1#" + expectedTimestamp, new String(rowKey));
    }

    @Test
    public void shouldReturnRowKeyForGivenInputWhenOffsetIsNonZero() {
        byte[] rowKey = longBowSchema.getKey(defaultRow, 1000L);

        long expectedTimestamp = Long.MAX_VALUE - (defaultTimestampInMillis - 1000L);
        assertEquals("driver1#" + expectedTimestamp, new String(rowKey));
    }

    @Test
    public void shouldReturnKeyForGivenInputAndTmestamp() {
        byte[] rowKey = longBowSchema.getAbsoluteKey(defaultRow, 1000L);
        long expectedTimestamp = Long.MAX_VALUE - 1000L;

        assertEquals("driver1#" + expectedTimestamp, new String(rowKey));
    }

    @Test
    public void shouldGetTheColumnSize() {
        assertEquals((Integer) 4, longBowSchema.getColumnSize());
    }

    @Test
    public void shouldGetTheIndexForColumn() {

        assertEquals((Integer) 0, longBowSchema.getIndex("longbow_key"));
    }

    @Test
    public void shouldGetColumnsForDataFilterForOneDataColumn() {
        assertEquals(Arrays.asList("longbow_data1"), longBowSchema.getColumnNames(s -> s.getKey().contains("longbow_data")));
    }

    @Test
    public void shouldGetColumnsForDataFilterForMultipleDataColumn() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);

        assertEquals(Arrays.asList("longbow_data1", "longbow_data2"), longBowSchema.getColumnNames(s -> s.getKey().contains("longbow_data")));
    }

    @Test
    public void shouldGetColumnsForNonDataFilterForMultipleDataColumn() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);

        assertEquals(Arrays.asList("longbow_key", "rowtime", "longbow_duration"), longBowSchema.getColumnNames(s -> !s.getKey().contains("longbow_data")));
    }

    @Test
    public void shouldGetDurationInMillisForMinutes() {
        int minutes = 20;
        Row rowsForMinutes = Row.of("driver1", "order1", defaultTimestamp, minutes + "m");
        assertEquals(minutes * 60 * 1000, longBowSchema.getDurationInMillis(rowsForMinutes));
    }

    @Test
    public void shouldGetDurationInMillisForHours() {
        int hours = 24;
        Row rowsForHours = Row.of("driver1", "order1", defaultTimestamp, hours + "h");
        assertEquals(hours * 60 * 60 * 1000, longBowSchema.getDurationInMillis(rowsForHours));
    }

    @Test
    public void shouldGetDurationInMillisForDays() {
        int days = 15;
        Row rowForDays = Row.of("driver1", "order1", defaultTimestamp, days + "d");
        assertEquals(days * 24 * 60 * 60 * 1000, longBowSchema.getDurationInMillis(rowForDays));
    }

    @Test
    public void shouldThrowExceptionForInvalidTimeUnit() {
        expectedException.expect(InvalidLongbowDurationException.class);
        expectedException.expectMessage("'15s' is a invalid duration string");

        int seconds = 15;
        Row rowsForMonths = Row.of("driver1", "order1", defaultTimestamp, seconds + "s");
        longBowSchema.getDurationInMillis(rowsForMonths);
    }

    @Test
    public void shouldReturnValueForAColumnAndInput() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);
        Row input = new Row(5);
        input.setField(0, "driver_id");
        assertEquals("driver_id", longBowSchema.getValue(input, "longbow_key"));
    }

    @Test
    public void shouldReturnIfItContainsTheColumn() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);
        assertTrue(longBowSchema.contains("longbow_key"));
        assertFalse(longBowSchema.contains("longbow_earliest"));
    }

    @Test
    public void shouldReturnTypeBasedOnColumnNames() {
        String[] firstColumnNames = {"longbow_read_key"};
        LongbowSchema firstLongBowSchema = new LongbowSchema(firstColumnNames);

        String[] secondColumnNames = {"longbow_write_key"};
        LongbowSchema secondLongBowSchema = new LongbowSchema(secondColumnNames);

        String[] thirdColumnNames = {"longbow_key"};
        LongbowSchema thirdLongbowSchema = new LongbowSchema(thirdColumnNames);
        assertEquals(LongbowType.LongbowRead, firstLongBowSchema.getType());
        assertEquals(LongbowType.LongbowWrite, secondLongBowSchema.getType());
        assertEquals(LongbowType.LongbowProcess, thirdLongbowSchema.getType());
    }

    @Test
    public void shouldThrowErrorIfNotIdentifyLongbowSchema() {
        String[] columnNames = {"test_key"};
        longBowSchema = new LongbowSchema(columnNames);

        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Unable to identify LongbowProcessor. Provide either "
                + LongbowType.LongbowProcess.getKeyName() + ", " + LongbowType.LongbowRead.getKeyName() + " or " + LongbowType.LongbowWrite.getKeyName());
        longBowSchema.getType();
    }

    @Test
    public void shouldDistinguishLongbowPlus() {
        String[] columnNames = {"longbow_key"};

        longBowSchema = new LongbowSchema(columnNames);
        assertFalse(longBowSchema.isLongbowPlus());
    }

}
