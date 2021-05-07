package io.odpf.dagger.core.processors.longbow;

import io.odpf.dagger.core.processors.longbow.validator.LongbowType;
import org.apache.flink.types.Row;

import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.exception.InvalidLongbowDurationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Timestamp;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

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
        defaultRow = getRow("driver1", "order1", defaultTimestamp, "24h");
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
        Row rowsForMinutes = getRow("driver1", "order1", defaultTimestamp, minutes + "m");
        assertEquals(minutes * 60 * 1000, longBowSchema.getDurationInMillis(rowsForMinutes));
    }

    @Test
    public void shouldGetDurationInMillisForHours() {
        int hours = 24;
        Row rowsForHours = getRow("driver1", "order1", defaultTimestamp, hours + "h");
        assertEquals(hours * 60 * 60 * 1000, longBowSchema.getDurationInMillis(rowsForHours));
    }

    @Test
    public void shouldGetDurationInMillisForDays() {
        int days = 15;
        Row rowForDays = getRow("driver1", "order1", defaultTimestamp, days + "d");
        assertEquals(days * 24 * 60 * 60 * 1000, longBowSchema.getDurationInMillis(rowForDays));
    }

    @Test
    public void shouldThrowExceptionForInvalidTimeUnit() {
        expectedException.expect(InvalidLongbowDurationException.class);
        expectedException.expectMessage("'15s' is a invalid duration string");

        int seconds = 15;
        Row rowsForMonths = getRow("driver1", "order1", defaultTimestamp, seconds + "s");
        longBowSchema.getDurationInMillis(rowsForMonths);
    }

    @Test
    public void shouldReturnValueForAColumnAndInput() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);
        Row input = new Row(5);
        input.setField(0, "driver_id");

        Assert.assertEquals(longBowSchema.getValue(input, "longbow_key"), "driver_id");
    }

    @Test
    public void shouldReturnIfItContainsTheColumn() {
        String[] columnNames = {"longbow_key", "longbow_data1", "rowtime", "longbow_duration", "longbow_data2"};
        longBowSchema = new LongbowSchema(columnNames);

        Assert.assertEquals(longBowSchema.contains("longbow_key"), true);
        Assert.assertEquals(longBowSchema.contains("longbow_earliest"), false);
    }

    @Test
    public void shouldReturnTypeBasedOnColumnNames() {
        String[] firstColumnNames = {"longbow_read_key"};
        LongbowSchema firstLongBowSchema = new LongbowSchema(firstColumnNames);

        String[] secondColumnNames = {"longbow_write_key"};
        LongbowSchema secondLongBowSchema = new LongbowSchema(secondColumnNames);

        String[] thirdColumnNames = {"longbow_key"};
        LongbowSchema thirdLongbowSchema = new LongbowSchema(thirdColumnNames);
        Assert.assertEquals(firstLongBowSchema.getType(), LongbowType.LongbowRead);
        Assert.assertEquals(secondLongBowSchema.getType(), LongbowType.LongbowWrite);
        Assert.assertEquals(thirdLongbowSchema.getType(), LongbowType.LongbowProcess);
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
        Assert.assertFalse(longBowSchema.isLongbowPlus());
    }

    private Row getRow(Object... dataList) {
        Row input = new Row(dataList.length);
        for (int i = 0; i < dataList.length; i++) {
            input.setField(i, dataList[i]);
        }
        return input;
    }
}
