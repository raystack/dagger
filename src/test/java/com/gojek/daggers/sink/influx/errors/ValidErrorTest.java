package com.gojek.daggers.sink.influx.errors;

import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.MockitoAnnotations.initMocks;

public class ValidErrorTest {
    @Mock
    private Iterable<Point> points;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldHaveError() {
        ValidError validError = new ValidError();
        Assert.assertTrue(validError.hasException());
    }

    @Test
    public void shouldFilterOnlyErrorNotExceptions() {
        ValidError validError = new ValidError();

        Assert.assertTrue(validError.filterError(new Error()));
        Assert.assertFalse(validError.filterError(new Exception()));
    }

    @Test
    public void shouldWrapErrorsInExceptions() {
        ValidError validError = new ValidError();
        validError.handle(points, new Error("Test"));
        Exception currentException = validError.getCurrentException();
        Assert.assertTrue(currentException instanceof Exception);
        Assert.assertEquals("java.lang.Error: Test", currentException.getMessage());
    }
}
