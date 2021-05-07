package io.odpf.dagger.core.processors.internal.processor.invalid;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class InvalidInternalConfigProcessorTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void canNotProcessWhenTypeIsNull() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        Assert.assertFalse(invalidInternalConfigProcessor.canProcess(null));
    }

    @Test
    public void canNotProcessWhenTypeIsEmpty() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        Assert.assertFalse(invalidInternalConfigProcessor.canProcess(""));
    }

    @Test
    public void canNotProcessForSqlConfiguration() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        Assert.assertFalse(invalidInternalConfigProcessor.canProcess("sql"));
    }

    @Test
    public void canNotProcessForRandomConfiguration() {
        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        Assert.assertFalse(invalidInternalConfigProcessor.canProcess("random_config"));
    }

    @Test
    public void shouldThrowExceptionWhenProcessNullConfig() {
        expectedException.expect(InvalidConfigurationException.class);
        expectedException.expectMessage("Invalid configuration, type '' for custom doesn't exists");

        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(null);

        invalidInternalConfigProcessor.process(null);
    }

    @Test
    public void shouldThrowExceptionWhenProcessNonNullConfig() {
        expectedException.expect(InvalidConfigurationException.class);
        expectedException.expectMessage("Invalid configuration, type 'invalid' for custom doesn't exists");

        InvalidInternalConfigProcessor invalidInternalConfigProcessor = new InvalidInternalConfigProcessor(new InternalSourceConfig("output", "value", "invalid"));

        invalidInternalConfigProcessor.process(null);
    }
}
