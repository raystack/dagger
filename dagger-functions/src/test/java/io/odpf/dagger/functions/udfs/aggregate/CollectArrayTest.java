package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.ArrayAccumulator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class CollectArrayTest {

    @Mock
    private ArrayAccumulator arrayaccumulator;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateArrayAccumulator() {
        CollectArray collectArray = new CollectArray();
        ArrayAccumulator accumulator = collectArray.createAccumulator();
        assertNotNull(accumulator);
        assertEquals(accumulator.getClass(), ArrayAccumulator.class);
    }

    @Test
    public void shouldAddEntriesToAccumulator() {
        CollectArray collectArray = new CollectArray();
        collectArray.accumulate(arrayaccumulator, "value1");
        collectArray.accumulate(arrayaccumulator, "value2");
        collectArray.accumulate(arrayaccumulator, "value3");

        verify(arrayaccumulator, times(3)).add(any(String.class));
    }

    @Test
    public void shouldReturnArrayOfRowFromAccumulator() {
        CollectArray collectArray = new CollectArray();
        collectArray.getValue(arrayaccumulator);

        verify(arrayaccumulator, times(1)).emit();
    }
}
