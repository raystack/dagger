package io.odpf.dagger.functions.udfs.scalar.dart.types;


import io.odpf.dagger.functions.exceptions.KeyDoesNotExistException;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MapCache extends Cache implements Serializable {

    public static final MapCache NULL_CACHE = new MapCache(new HashMap<String, String>(), null);
    private Map<String, String> cache;

    public MapCache(Map<String, String> cache, Date timeOfCaching) {
        super(timeOfCaching);
        this.cache = cache;
    }

    public MapCache(Map<String, String> cache) {
        this(cache, new Date());
    }

    public String get(String key) {
        String value = cache.get(key);
        if (value == null) {
            throw new KeyDoesNotExistException(String.format("Key \"%s\" does not exist", key));
        }
        return value;
    }

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
