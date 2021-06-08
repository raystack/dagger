package io.odpf.dagger.functions.udfs.scalar.dart.types;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class SetCache extends Cache implements Serializable {
    public static final SetCache NULL_CACHE = new SetCache(new HashSet<>(), null);
    private Set<String> cache;

    public SetCache(Set<String> cache) {
        this(cache, new Date());
    }

    private SetCache(Set<String> cache, Date timeOfCaching) {
        super(timeOfCaching);
        this.cache = cache;
    }

    public boolean contains(String data) {
        return cache.contains(data);
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

        SetCache setCache = (SetCache) o;

        return cache != null ? cache.equals(setCache.cache) : setCache.cache == null;
    }

    @Override
    public int hashCode() {
        return cache != null ? cache.hashCode() : 0;
    }

    public boolean matches(String field, String regex) {
        return cache
                .stream()
                .anyMatch(s -> Pattern.matches(String.format(regex, s), field));
    }
}
