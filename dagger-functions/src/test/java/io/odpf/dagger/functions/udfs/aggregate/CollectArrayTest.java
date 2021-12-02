package io.odpf.dagger.functions.udfs.aggregate;

import io.odpf.dagger.functions.udfs.aggregate.accumulator.ArrayAccumulator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
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

    @Test
    public void shouldMergeAccumulators() {
        CollectArray collectArray = new CollectArray();
        ArrayAccumulator arrayAccumulator1 = new ArrayAccumulator();
        collectArray.accumulate(arrayAccumulator1, "value1");
        collectArray.accumulate(arrayAccumulator1, "value2");
        collectArray.accumulate(arrayAccumulator1, "value3");

        ArrayAccumulator arrayAccumulator2 = new ArrayAccumulator();
        ArrayAccumulator arrayAccumulator3 = new ArrayAccumulator();

        arrayAccumulator2.add("value4");
        arrayAccumulator2.add("value5");
        arrayAccumulator3.add("value6");
        arrayAccumulator3.add("value7");

        Iterable<ArrayAccumulator> iterable = mock(Iterable.class);
        Iterator<ArrayAccumulator> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(arrayAccumulator2).thenReturn(arrayAccumulator3);

        collectArray.merge(arrayAccumulator1, iterable);

        List<Object> result = collectArray.getValue(arrayAccumulator1);
        assertEquals(7, result.size());
        assertEquals("value1", result.get(0));
        assertEquals("value2", result.get(1));
        assertEquals("value3", result.get(2));
        assertEquals("value4", result.get(3));
        assertEquals("value5", result.get(4));
        assertEquals("value6", result.get(5));
        assertEquals("value7", result.get(6));
    }

    @Test
    public void shouldNotChangeAccumulatorIfIterableIsEmptyOnMerge() {
        CollectArray collectArray = new CollectArray();
        ArrayAccumulator arrayAccumulator = new ArrayAccumulator();
        collectArray.accumulate(arrayAccumulator, "value1");
        collectArray.accumulate(arrayAccumulator, "value2");
        collectArray.accumulate(arrayAccumulator, "value3");


        Iterable<ArrayAccumulator> iterable = mock(Iterable.class);
        Iterator<ArrayAccumulator> iterator = mock(Iterator.class);
        when(iterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(new ArrayAccumulator());

        collectArray.merge(arrayAccumulator, iterable);

        List<Object> result = collectArray.getValue(arrayAccumulator);
        assertEquals(3, result.size());
        assertEquals("value1", result.get(0));
        assertEquals("value2", result.get(1));
        assertEquals("value3", result.get(2));
    }
}
