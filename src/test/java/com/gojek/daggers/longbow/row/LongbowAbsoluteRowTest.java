package com.gojek.daggers.longbow.row;

import com.gojek.daggers.longbow.LongbowSchema;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowAbsoluteRowTest {
    @Mock
    private LongbowSchema longbowSchema;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnLatestRow() {
        Row input = new Row(1);
        when(longbowSchema.getValue(input, "longbow_latest")).thenReturn(String.valueOf(1000L));
        LongbowAbsoluteRow longbowAbsoluteRow = new LongbowAbsoluteRow(longbowSchema);
        longbowAbsoluteRow.getLatest(input);

        verify(longbowSchema, times(1)).getAbsoluteKey(input, 1000L);
    }

    @Test
    public void shouldReturnEarliestRow() {
        Row input = new Row(1);
        when(longbowSchema.getValue(input, "longbow_earliest")).thenReturn(String.valueOf(1000L));
        LongbowAbsoluteRow longbowAbsoluteRow = new LongbowAbsoluteRow(longbowSchema);
        longbowAbsoluteRow.getEarliest(input);

        verify(longbowSchema, times(1)).getAbsoluteKey(input, 1000L);
    }

    @Test
    public void shouldReturnInvalidFields() {
        LongbowAbsoluteRow longbowAbsoluteRow = new LongbowAbsoluteRow(longbowSchema);
        Assert.assertArrayEquals(longbowAbsoluteRow.getInvalidFields(), new String[]{"longbow_duration"});
    }
}