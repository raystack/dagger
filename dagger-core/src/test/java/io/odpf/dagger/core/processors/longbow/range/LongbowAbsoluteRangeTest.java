package io.odpf.dagger.core.processors.longbow.range;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowAbsoluteRangeTest {
    @Mock
    private LongbowSchema longbowSchema;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnUpperBound() {
        Row input = new Row(1);
        when(longbowSchema.getValue(input, "longbow_latest")).thenReturn(1000L);
        LongbowAbsoluteRange longbowAbsoluteRow = new LongbowAbsoluteRange(longbowSchema);
        longbowAbsoluteRow.getUpperBound(input);

        verify(longbowSchema, times(1)).getAbsoluteKey(input, 1000L);
    }

    @Test
    public void shouldReturnLowerBound() {
        Row input = new Row(1);
        when(longbowSchema.getValue(input, "longbow_earliest")).thenReturn(1000L);
        LongbowAbsoluteRange longbowAbsoluteRow = new LongbowAbsoluteRange(longbowSchema);
        longbowAbsoluteRow.getLowerBound(input);

        verify(longbowSchema, times(1)).getAbsoluteKey(input, 1000L);
    }

    @Test
    public void shouldReturnInvalidFields() {
        LongbowAbsoluteRange longbowAbsoluteRow = new LongbowAbsoluteRange(longbowSchema);
        assertArrayEquals(new String[]{"longbow_duration"}, longbowAbsoluteRow.getInvalidFields());
    }
}
