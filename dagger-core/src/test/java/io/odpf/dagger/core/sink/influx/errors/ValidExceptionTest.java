package io.odpf.dagger.core.sink.influx.errors;

import org.influxdb.dto.Point;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class ValidExceptionTest {

    @Test
    public void shouldHaveError() {
        ValidException validException = new ValidException();
        assertTrue(validException.hasException());
    }

    @Test
    public void shouldFilterException() {
        ValidException validException = new ValidException();

        assertTrue(validException.filterError(new Exception()));
    }

    @Test
    public void shouldNotFilterError() {
        ValidException validException = new ValidException();

        assertFalse(validException.filterError(new Error()));
    }

    @Test
    public void shouldWrapErrorsInExceptions() {
        Iterable<Point> points = Collections.emptyList();
        ValidException validException = new ValidException();
        validException.handle(points, new Exception("Test"));
        Exception currentException = validException.getCurrentException();
        assertTrue(currentException instanceof Exception);
        System.out.println(currentException.getMessage());
        assertEquals("java.lang.Exception: Test", currentException.getMessage());
    }
}
