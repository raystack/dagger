package io.odpf.dagger.core.processors.longbow.range;

import io.odpf.dagger.core.processors.longbow.LongbowSchema;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class LongbowDurationRangeTest {

    @Mock
    private LongbowSchema longbowSchema;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnUpperBound() {
        LongbowDurationRange longbowDurationRow = new LongbowDurationRange(longbowSchema);
        Row input = new Row(1);
        longbowDurationRow.getUpperBound(input);

        verify(longbowSchema, times(1)).getKey(input, 0);
    }

    @Test
    public void shouldReturnLowerBound() {
        Row input = new Row(1);
        when(longbowSchema.getDurationInMillis(input)).thenReturn(100L);
        LongbowDurationRange longbowDurationRow = new LongbowDurationRange(longbowSchema);
        longbowDurationRow.getLowerBound(input);

        verify(longbowSchema, times(1)).getKey(input, 100L);
    }

    @Test
    public void shouldReturnInvalidFields() {
        LongbowDurationRange longbowDurationRow = new LongbowDurationRange(longbowSchema);
        assertArrayEquals(new String[]{"longbow_earliest", "longbow_latest"}, longbowDurationRow.getInvalidFields());
    }

}
