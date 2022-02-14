package io.odpf.dagger.core.processors.internal.processor.function.functions;

import org.junit.Test;
import java.sql.Timestamp;
import static org.junit.Assert.*;


public class CurrentTimestampFunctionTest {
    @Test
    public void canNotProcessWhenFunctionNameIsNull() {
        CurrentTimestampFunction currentTimestampFunction = new CurrentTimestampFunction(null, null);
        assertFalse(currentTimestampFunction.canProcess(null));
    }

    @Test
    public void canNotProcessWhenFunctionNameIsDifferent() {
        CurrentTimestampFunction currentTimestampFunction = new CurrentTimestampFunction(null, null);
        assertFalse(currentTimestampFunction.canProcess("JSON_PAYLOAD"));
    }

    @Test
    public void canProcessWhenFunctionNameIsCorrect() {
        CurrentTimestampFunction currentTimestampFunction = new CurrentTimestampFunction(null, null);
        assertTrue(currentTimestampFunction.canProcess("CURRENT_TIMESTAMP"));
    }

    @Test
    public void shouldGetCurrentTimestampAsResult() {
        long testTimestampLong = System.currentTimeMillis();
        CurrentTimestampFunction currentTimestampFunction = new CurrentTimestampFunction(null, null);

        Timestamp currentTimestamp = (Timestamp) currentTimestampFunction.getResult(null);
        assertTrue((currentTimestamp.getTime() - testTimestampLong) < 10);  // difference is less than 10ms;
    }
}
