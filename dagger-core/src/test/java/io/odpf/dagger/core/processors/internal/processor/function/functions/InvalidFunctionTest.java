package io.odpf.dagger.core.processors.internal.processor.function.functions;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InvalidFunctionTest {
    @Test
    public void canNotProcessWhenFunctionNameIsNull() {
        InvalidFunction invalidFunction = new InvalidFunction(null);
        assertFalse(invalidFunction.canProcess(null));
    }

    @Test
    public void canNotProcessWhenFunctionNameIsEmpty() {
        InvalidFunction invalidFunction = new InvalidFunction(null);
        assertFalse(invalidFunction.canProcess(""));
    }

    @Test
    public void canNotProcessForAnyFunctionName() {
        InvalidFunction invalidFunction = new InvalidFunction(null);
        assertFalse(invalidFunction.canProcess("ANY_RANDOM_FUNCTION"));
    }

    @Test
    public void shouldThrowExceptionWhenGettingResultForInvalidInternalSourceConfig() {
        InvalidFunction invalidFunction = new InvalidFunction(null);

        Row row = new Row(2);
        RowManager rowManager = new RowManager(row);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> invalidFunction.getResult(rowManager));
        assertEquals("The function \"\" is not supported in custom configuration",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenGettingResultForValidInternalSourceConfig() {
        InternalSourceConfig internalSourceConfig = mock(InternalSourceConfig.class);
        when(internalSourceConfig.getValue()).thenReturn("UNNEST");

        InvalidFunction invalidFunction = new InvalidFunction(internalSourceConfig);

        Row row = new Row(2);
        RowManager rowManager = new RowManager(row);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> invalidFunction.getResult(rowManager));
        assertEquals("The function \"UNNEST\" is not supported in custom configuration",
                invalidConfigException.getMessage());
    }
}
