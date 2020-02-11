package com.gojek.daggers.postProcessors.longbow;

import com.gojek.daggers.exception.DaggerConfigurationException;
import com.gojek.daggers.exception.InvalidLongbowDurationException;
import com.gojek.daggers.postProcessors.longbow.row.LongbowDurationRow;
import org.apache.flink.types.Row;
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
    private LongbowDurationRow longbowDurationRow = new LongbowDurationRow(longBowSchema);

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
        expectedException.expectMessage("'15m' is a invalid duration string");

        int months = 15;
        Row rowsForMonths = getRow("driver1", "order1", defaultTimestamp, months + "m");
        longBowSchema.getDurationInMillis(rowsForMonths);
    }

    @Test
    public void shouldValidateWhenEventTimestampIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'event_timestamp'");

        String[] columnNames = {"rowtime", "longbow_key", "longbow_duration", "longbow_data1"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);

        longBowSchema.validateMandatoryFields(longbowDurationRow);
    }

    @Test
    public void shouldValidateWhenRowtimeIsMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'rowtime'");

        String[] columnNames = {"longbow_data1", "longbow_key", "longbow_duration", "event_timestamp"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);

        longBowSchema.validateMandatoryFields(longbowDurationRow);
    }

    @Test
    public void shouldValidateWhenMultipleFieldsAreMissingInQuery() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Missing required field: 'event_timestamp,rowtime'");

        String[] columnNames = {"longbow_data1", "longbow_key", "longbow_duration"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);

        longBowSchema.validateMandatoryFields(longbowDurationRow);
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
    public void shouldValidateWhenDurationAndEarliestBothAreProvided() {
        expectedException.expect(DaggerConfigurationException.class);
        expectedException.expectMessage("Invalid fields: 'longbow_earliest'");

        String[] columnNames = {"longbow_data1", "longbow_key", "event_timestamp", "rowtime", "longbow_duration", "longbow_earliest"};
        LongbowSchema longBowSchema = new LongbowSchema(columnNames);

        longBowSchema.validateMandatoryFields(longbowDurationRow);
    }

    private Row getRow(Object... dataList) {
        Row input = new Row(dataList.length);
        for (int i = 0; i < dataList.length; i++) {
            input.setField(i, dataList[i]);
        }
        return input;
    }


}