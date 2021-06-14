package io.odpf.dagger.functions.udfs.scalar.dart.types;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The type Set cache.
 */
public class SetCache extends Cache implements Serializable {
    /**
     * The constant NULL_CACHE.
     */
    public static final SetCache NULL_CACHE = new SetCache(new HashSet<>(), null);
    private Set<String> cache;

    /**
     * Instantiates a new Set cache.
     *
     * @param cache the cache
     */
    public SetCache(Set<String> cache) {
        this(cache, new Date());
    }

    private SetCache(Set<String> cache, Date timeOfCaching) {
        super(timeOfCaching);
        this.cache = cache;
    }

    /**
     * Contains boolean.
     *
     * @param data the data
     * @return the boolean
     */
    public boolean contains(String data) {
        return cache.contains(data);
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

        SetCache setCache = (SetCache) o;

        return cache != null ? cache.equals(setCache.cache) : setCache.cache == null;
    }

    @Override
    public int hashCode() {
        return cache != null ? cache.hashCode() : 0;
    }

    /**
     * Matches boolean.
     *
     * @param field the field
     * @param regex the regex
     * @return the boolean
     */
    public boolean matches(String field, String regex) {
        return cache
                .stream()
                .anyMatch(s -> Pattern.matches(String.format(regex, s), field));
    }
}
