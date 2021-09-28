package io.odpf.dagger.core.sink.influx.errors;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class NoErrorTest {

    @Test
    public void shouldNotReturnAnyError() {
        NoError noError = new NoError();
        assertNull(noError.getCurrentException());
    }

    @Test
    public void shouldHaveNoError() {
        NoError noError = new NoError();
        assertFalse(noError.hasException());
    }

    @Test
    public void shouldFilterAllErrors() {
        NoError noError = new NoError();

        assertFalse(noError.filterError(new Throwable()));
    }
}
