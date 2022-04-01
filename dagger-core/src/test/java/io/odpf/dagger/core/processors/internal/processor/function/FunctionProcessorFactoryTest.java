package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.function.functions.CurrentTimestampFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.InvalidFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.JsonPayloadFunction;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
}
