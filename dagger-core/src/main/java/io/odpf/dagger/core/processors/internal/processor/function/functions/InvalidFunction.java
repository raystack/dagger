package io.odpf.dagger.core.processors.internal.processor.function.functions;

import io.odpf.dagger.core.exception.InvalidFunctionException;

import io.odpf.dagger.core.processors.internal.processor.function.FunctionProcessor;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.common.RowManager;

public class InvalidFunction implements FunctionProcessor {
    private InternalSourceConfig internalSourceConfig;

    /**
     * Instantiates a new Invalid internal function processor.
     *
     * @param internalSourceConfig the internal source config
     * @param configuration the dagger config
     */
    public InvalidInternalConfigProcessor(InternalSourceConfig internalSourceConfig, Config configuration) {
        this.internalSourceConfig = internalSourceConfig;
        this.configuration = configuration;
    }

    @Override
    public boolean canProcess(String functionName) {
        return false;
    }

    public void process(RowManager rowManager) {
        String type = "";
        throw new InvalidFunctionException(String.format("Invalid configuration, function '%s' for internal postprocessor does not exist", type));
    }
    
}
