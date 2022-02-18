package io.odpf.dagger.core.processors.internal.processor.invalid;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import org.junit.Test;

import static org.junit.Assert.*;

public class InvalidInternalConfigProcessorTest {

    @Test
    public void canNotProcessWhenTypeIsNull() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        assertFalse(invalidInternalConfigProcessor.canProcess(null));
    }

    @Test
    public void canNotProcessWhenTypeIsEmpty() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        assertFalse(invalidInternalConfigProcessor.canProcess(""));
    }

    @Test
    public void canNotProcessForSqlConfiguration() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        assertFalse(invalidInternalConfigProcessor.canProcess("sql"));
    }

    @Test
    public void canNotProcessForRandomConfiguration() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        assertFalse(invalidInternalConfigProcessor.canProcess("random_config"));
    }

    @Test
    public void shouldThrowExceptionWhenProcessNullConfig() {

        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> invalidInternalConfigProcessor.process(null));
        assertEquals("Invalid configuration, type '' for custom doesn't exists",
                invalidConfigException.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenProcessNonNullConfig() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(new InternalSourceConfig("output", "value", "invalid", null));

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class,
                () -> invalidInternalConfigProcessor.process(null));
        assertEquals("Invalid configuration, type 'invalid' for custom doesn't exists",
                invalidConfigException.getMessage());
    }
}
