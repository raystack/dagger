package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.function.functions.CurrentTimestampFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.JsonPayloadFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.InvalidFunction;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;

/**
 * The factory class for internal function post processors.
 */
public class FunctionProcessorFactory {
    private FunctionProcessorFactory() {
        throw new IllegalStateException("Factory class");
    }

    private static List<FunctionProcessor> getFunctions(InternalSourceConfig internalSourceConfig, SchemaConfig schemaConfig) {
        Clock clock = Clock.systemDefaultZone();
        return Arrays.asList(new CurrentTimestampFunction(clock),
                new JsonPayloadFunction(internalSourceConfig, schemaConfig));
    }

    /**
     * Gets function for post-processing.
     *
     * @param internalSourceConfig the internal source config
     * @param schemaConfig         the schema config
     *
     * @return the function processor
     */
    public static FunctionProcessor getFunctionProcessor(InternalSourceConfig internalSourceConfig, SchemaConfig schemaConfig) {
        return getFunctions(internalSourceConfig, schemaConfig)
                .stream()
                .filter(functionProcessor -> functionProcessor.canProcess(internalSourceConfig.getValue()))
                .findFirst()
                .orElse(new InvalidFunction(internalSourceConfig));
    }
}
