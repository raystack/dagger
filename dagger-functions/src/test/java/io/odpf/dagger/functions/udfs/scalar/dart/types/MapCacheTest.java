package io.odpf.dagger.functions.udfs.scalar.dart.types;

import io.odpf.dagger.functions.exceptions.KeyDoesNotExistException;
import org.apache.commons.collections.map.SingletonMap;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapCacheTest {
    private Map<String, String> someContent;
    private final String defaultKey = "key";
    private final String defaultValue = "value";

    @Before
    public void setUp() {
        someContent = new HashMap<String, String>(new SingletonMap(defaultKey, defaultValue));
    }

    @Test
    public void nullCacheShouldBeExpired() {
        MapCache nullCache = MapCache.NULL_CACHE;

        assertEquals(true, nullCache.hasExpired(-1));
    }

    @Test
    public void shouldReturnValueWhenKeyExist() {
        MapCache mapCache = new MapCache(someContent);

        assertEquals(defaultValue, mapCache.get(defaultKey));
    }

    @Test(expected = KeyDoesNotExistException.class)
    public void shouldThrowErrorWhenKeyDoesNotExist() {
        MapCache mapCache = new MapCache(someContent);

        mapCache.get("invalidKey");
    }

    @Test
    public void shouldExpireContentWhenTimeElapsed() {
        MapCache mapCache = new MapCache(someContent);

        assertEquals(true, mapCache.hasExpired(-1));
    }

    @Test
    public void shouldNotExpireContentWhenTimeHasNotElapsed() {
        MapCache mapCache = new MapCache(someContent);

        assertEquals(false, mapCache.hasExpired(1));
    }
}
