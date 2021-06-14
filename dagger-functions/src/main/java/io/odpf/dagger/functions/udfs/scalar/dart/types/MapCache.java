package io.odpf.dagger.functions.udfs.scalar.dart.types;


import io.odpf.dagger.functions.exceptions.KeyDoesNotExistException;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The type Map cache.
 */
public class MapCache extends Cache implements Serializable {

    /**
     * The constant NULL_CACHE.
     */
    public static final MapCache NULL_CACHE = new MapCache(new HashMap<String, String>(), null);
    private Map<String, String> cache;

    /**
     * Instantiates a new Map cache.
     *
     * @param cache         the cache
     * @param timeOfCaching the time of caching
     */
    public MapCache(Map<String, String> cache, Date timeOfCaching) {
        super(timeOfCaching);
        this.cache = cache;
    }

    /**
     * Instantiates a new Map cache.
     *
     * @param cache the cache
     */
    public MapCache(Map<String, String> cache) {
        this(cache, new Date());
    }

    /**
     * Get string.
     *
     * @param key the key
     * @return the string
     */
    public String get(String key) {
        String value = cache.get(key);
        if (value == null) {
            throw new KeyDoesNotExistException(String.format("Key \"%s\" does not exist", key));
        }
        return value;
    }

    /**
     * Is empty boolean.
     *
     * @return the boolean
     */
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MapCache mapCache = (MapCache) o;
        return Objects.equals(cache, mapCache.cache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cache);
    }
}
