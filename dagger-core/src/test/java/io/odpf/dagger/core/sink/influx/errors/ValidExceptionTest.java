package io.odpf.dagger.core.sink.influx.errors;

import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.MockitoAnnotations.initMocks;

public class ValidExceptionTest {

    @Mock
    private Iterable<Point> points;

    @Before
    public void setUp() {
        initMocks(this);
    }

    @Test
    public void shouldHaveError() {
        ValidException validException = new ValidException();
        Assert.assertTrue(validException.hasException());
    }

    @Test
    public void shouldFilterException() {
        ValidException validException = new ValidException();

        Assert.assertTrue(validException.filterError(new Exception()));
    }

    @Test
    public void shouldNotFilterError() {
        ValidException validException = new ValidException();

        Assert.assertFalse(validException.filterError(new Error()));
    }

    @Test
    public void shouldWrapErrorsInExceptions() {
        ValidException validException = new ValidException();
        validException.handle(points, new Exception("Test"));
        Exception currentException = validException.getCurrentException();
        Assert.assertTrue(currentException instanceof Exception);
        Assert.assertEquals("Test", currentException.getMessage());
    }
}
