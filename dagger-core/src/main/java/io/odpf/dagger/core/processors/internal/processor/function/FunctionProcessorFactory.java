package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.function.functions.CurrentTimestampFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.JsonPayloadFunction;
import io.odpf.dagger.core.processors.internal.processor.function.functions.InvalidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * The factory class for internal function post processors.
 */
public class FunctionProcessorFactory {
    private FunctionProcessorFactory() {
        throw new IllegalStateException("Factory class");
    }

    private static List<FunctionProcessor> getFunctions(Configuration configuration) {
        return Arrays.asList(new CurrentTimestampFunction(),
                new JsonPayloadFunction(configuration));
    }

    /**
     * Gets function for post-processing.
     *
     * @param internalSourceConfig the internal source config
     * @param configuration        the dagger configuration
     *
     * @return the function processor
     */
    public static FunctionProcessor getFunctionProcessor(InternalSourceConfig internalSourceConfig, Configuration configuration) {
        return getFunctions(configuration)
                .stream()
                .filter(functionProcessor -> functionProcessor.canProcess(internalSourceConfig.getValue()))
                .findFirst()
                .orElse(new InvalidFunction(internalSourceConfig, configuration));
    }
}
