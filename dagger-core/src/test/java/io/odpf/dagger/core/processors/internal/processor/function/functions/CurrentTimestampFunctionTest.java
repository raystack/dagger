package io.odpf.dagger.core.processors.internal.processor.function.functions;

import org.junit.Test;

import java.sql.Timestamp;
import java.time.Clock;


import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CurrentTimestampFunctionTest {
    @Test
    public void canNotProcessWhenFunctionNameIsNull() {
        CurrentTimestampFunction currentTimestampFunction = new CurrentTimestampFunction(null);
        assertFalse(currentTimestampFunction.canProcess(null));
    }

    @Test
    public void canNotProcessWhenFunctionNameIsDifferent() {
        CurrentTimestampFunction currentTimestampFunction = new CurrentTimestampFunction(null);
        assertFalse(currentTimestampFunction.canProcess("JSON_PAYLOAD"));
    }

    @Test
    public void canProcessWhenFunctionNameIsCorrect() {
        CurrentTimestampFunction currentTimestampFunction = new CurrentTimestampFunction(null);
        assertTrue(currentTimestampFunction.canProcess("CURRENT_TIMESTAMP"));
    }

    @Test
    public void shouldGetCurrentTimestampAsResult() {
        long currentTimestampMs = System.currentTimeMillis();
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(currentTimestampMs);

        CurrentTimestampFunction currentTimestampFunction = new CurrentTimestampFunction(clock);

        Timestamp expectedCurrentTimestamp = new Timestamp(currentTimestampMs);
        Timestamp actualCurrentTimestamp = (Timestamp) currentTimestampFunction.getResult(null);

        assertEquals(expectedCurrentTimestamp, actualCurrentTimestamp);
    }
}
