package io.odpf.dagger.core.sink.influx.errors;

import org.influxdb.dto.Point;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class ValidErrorTest {

    @Test
    public void shouldHaveError() {
        ValidError validError = new ValidError();
        assertTrue(validError.hasException());
    }

    @Test
    public void shouldFilterOnlyError() {
        ValidError validError = new ValidError();

        assertTrue(validError.filterError(new Error()));
    }

    @Test
    public void shouldNotFilterException() {
        ValidError validError = new ValidError();

        assertFalse(validError.filterError(new Exception()));
    }

    @Test
    public void shouldWrapErrorsInExceptions() {
        Iterable<Point> points = Collections.emptyList();
        ValidError validError = new ValidError();
        validError.handle(points, new Error("Test"));
        Exception currentException = validError.getCurrentException();
        assertTrue(currentException instanceof Exception);
        assertEquals("java.lang.Error: Test", currentException.getMessage());
    }
}
