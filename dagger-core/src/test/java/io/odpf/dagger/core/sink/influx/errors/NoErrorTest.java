package io.odpf.dagger.core.sink.influx.errors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class NoErrorTest {

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldNotReturnAnyError() {
        NoError noError = new NoError();
        Assert.assertNull(noError.getCurrentException());
    }

    @Test
    public void shouldHaveNoError() {
        NoError noError = new NoError();
        Assert.assertFalse(noError.hasException());
    }

    @Test
    public void shouldFilterAllErrors() {
        NoError noError = new NoError();

        Assert.assertFalse(noError.filterError(new Throwable()));
    }
}
