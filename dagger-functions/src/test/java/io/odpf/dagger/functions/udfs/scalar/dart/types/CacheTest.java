package io.odpf.dagger.functions.udfs.scalar.dart.types;

import org.junit.Before;
import org.junit.Test;
import java.util.Date;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CacheTest {
    private Cache cache;
    @Before
    public void setUp() {
        cache = new Cache(new Date()) { };
    }

    @Test
    public void shouldNotExpireContentWhenTimeHasNotElapsed() {

        assertFalse(cache.hasExpired(1));
    }

    @Test
    public void shouldExpireContentWhenTimeElapsed() {
        cache = new Cache(new Date(1535546718115L)) { };

        assertTrue(cache.hasExpired(1));
    }
}
