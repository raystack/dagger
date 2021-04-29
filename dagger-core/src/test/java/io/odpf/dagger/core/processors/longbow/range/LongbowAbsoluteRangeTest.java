package io.odpf.dagger.core.processors.longbow.range;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
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
        Assert.assertArrayEquals(longbowAbsoluteRow.getInvalidFields(), new String[]{"longbow_duration"});
    }
}
