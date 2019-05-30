package com.gojek.daggers.longbow;

import com.gojek.daggers.exception.InvalidLongbowDurationException;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class LongBowSchemaTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private HashMap<String, Integer> columnIndexMap;
    private LongBowSchema longBowSchema;
    private Row defaultRow;
    private Long defaultTimestampInMillis;
    private Timestamp defaultTimestamp;

    @Before
    public void setup() {
        columnIndexMap = new HashMap<>();
        columnIndexMap.put("longbow_key", 0);
        columnIndexMap.put("longbow_data1", 1);
        columnIndexMap.put("rowtime", 2);
        columnIndexMap.put("longbow_duration", 3);
        defaultTimestampInMillis = 433321L;


        longBowSchema = new LongBowSchema(columnIndexMap);
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
    public void shouldGetTheColumnSize() {
        assertEquals((Integer) 4, longBowSchema.getColumnSize());
    }

    @Test
    public void shouldGetTheIndexForColumn() {

        assertEquals((Integer) 0, longBowSchema.getIndex("longbow_key"));
    }

    @Test
    public void shouldGetColumnsForDataFilterForOneDataColumn() {
        assertEquals(Arrays.asList("longbow_data1"), longBowSchema.getColumns(s -> s.getKey().contains("longbow_data")));
    }

    @Test
    public void shouldGetColumnsForDataFilterForMultipleDataColumn() {
        columnIndexMap.put("longbow_data2", 5);

        assertEquals(Arrays.asList("longbow_data1", "longbow_data2"), longBowSchema.getColumns(s -> s.getKey().contains("longbow_data")));
    }

    @Test
    public void shouldGetColumnsForNonDataFilterForMultipleDataColumn() {
        columnIndexMap.put("longbow_data2", 5);

        assertEquals(Arrays.asList("longbow_key", "rowtime", "longbow_duration"), longBowSchema.getColumns(s -> !s.getKey().contains("longbow_data")));
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
        expectedException.expectMessage("15m is a invalid longbow duration");

        int months = 15;
        Row rowsForMonths = getRow("driver1", "order1", defaultTimestamp, months + "m");
        longBowSchema.getDurationInMillis(rowsForMonths);
    }

    private Row getRow(Object... dataList) {
        Row input = new Row(dataList.length);
        for (int i = 0; i < dataList.length; i++) {
            input.setField(i, dataList[i]);
        }
        return input;
    }


}