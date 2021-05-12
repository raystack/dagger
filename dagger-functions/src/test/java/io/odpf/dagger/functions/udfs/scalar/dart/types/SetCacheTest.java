package io.odpf.dagger.functions.udfs.scalar.dart.types;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SetCacheTest {
    private Set<String> someContent;

    @Before
    public void setUp() {
        someContent = new HashSet<>(Arrays.asList("item1", "item2"));
    }

    @Test
    public void nullCacheShouldBeExpired() {
        SetCache nullCache = SetCache.NULL_CACHE;

        assertEquals(true, nullCache.hasExpired(-1));
    }

    @Test
    public void shouldContainElementWhenListContainsIt() {
        SetCache setCache = new SetCache(someContent);

        assertEquals(true, setCache.contains("item1"));
    }

    @Test
    public void shouldNotContainElementWhenListDoesNotContainsIt() {
        SetCache setCache = new SetCache(someContent);

        assertEquals(false, setCache.contains("invalidItem"));
    }

    @Test
    public void shouldExpireContentWhenTimeElapsed() {
        SetCache setCache = new SetCache(someContent);

        assertEquals(true, setCache.hasExpired(-1));
    }

    @Test
    public void shouldNotExpireContentWhenTimeHasNotElapsed() {
        SetCache setCache = new SetCache(someContent);

        assertEquals(false, setCache.hasExpired(1));
    }
}
