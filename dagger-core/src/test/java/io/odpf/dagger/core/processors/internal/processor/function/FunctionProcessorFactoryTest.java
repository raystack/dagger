package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.function.functions.CurrentTimestampFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.InvalidFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.JsonPayloadFunction;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class FunctionProcessorFactoryTest {
    @Test
    public void shouldGetCurrentTimeFunctionProcessor() {
        InternalSourceConfig internalSourceConfig = mock(InternalSourceConfig.class);
        when(internalSourceConfig.getValue()).thenReturn("CURRENT_TIMESTAMP");

        FunctionProcessor functionProcessor = FunctionProcessorFactory.getFunctionProcessor(internalSourceConfig, null);

        assertEquals(CurrentTimestampFunction.class, functionProcessor.getClass());
    }

    @Test
    public void shouldGetJSONPayloadFunctionProcessor() {
        InternalSourceConfig internalSourceConfig = mock(InternalSourceConfig.class);
        when(internalSourceConfig.getValue()).thenReturn("JSON_PAYLOAD");

        FunctionProcessor functionProcessor = FunctionProcessorFactory.getFunctionProcessor(internalSourceConfig, null);

        assertEquals(JsonPayloadFunction.class, functionProcessor.getClass());
    }

    @Test
    public void shouldGetInvalidFunctionProcessor() {
        InternalSourceConfig internalSourceConfig = mock(InternalSourceConfig.class);
        when(internalSourceConfig.getValue()).thenReturn("UNNEST");

        FunctionProcessor functionProcessor = FunctionProcessorFactory.getFunctionProcessor(internalSourceConfig, null);

        assertEquals(InvalidFunction.class, functionProcessor.getClass());
    }


    @Ignore("move to JsonPayloadFunction tests")
    @Test
    public void shouldThrowInvalidConfigurationExceptionWhenInvalidDaggerConfigProvided() {
        InternalSourceConfig internalSourceConfig = mock(InternalSourceConfig.class);
        when(internalSourceConfig.getValue()).thenReturn("JSON_PAYLOAD");
        Configuration invalidConfiguration = mock(Configuration.class);

        InvalidConfigurationException invalidConfigException = assertThrows(InvalidConfigurationException.class, () -> {
            FunctionProcessorFactory.getFunctionProcessor(internalSourceConfig, null);
        });
        assertEquals("Invalid configuration: STREAMS not provided",
                invalidConfigException.getMessage());
    }
}
