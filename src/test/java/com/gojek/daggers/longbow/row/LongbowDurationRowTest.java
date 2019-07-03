package com.gojek.daggers.longbow.row;

import com.gojek.daggers.longbow.LongbowSchema;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowDurationRowTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnLatestRow() {
        LongbowDurationRow longbowDurationRow = new LongbowDurationRow(longbowSchema);
        Row input = new Row(1);
        longbowDurationRow.getLatest(input);

        verify(longbowSchema, times(1)).getKey(input, 0);
    }

    @Test
    public void shouldReturnEarliestRow() {
        Row input = new Row(1);
        when(longbowSchema.getDurationInMillis(input)).thenReturn(100L);
        LongbowDurationRow longbowDurationRow = new LongbowDurationRow(longbowSchema);
        longbowDurationRow.getEarliest(input);

        verify(longbowSchema, times(1)).getKey(input, 100L);
    }

    @Test
    public void shouldReturnInvalidFields() {
        LongbowDurationRow longbowDurationRow = new LongbowDurationRow(longbowSchema);
        Assert.assertArrayEquals(longbowDurationRow.getInvalidFields(), new String[]{"longbow_earliest", "longbow_latest"});
    }

}