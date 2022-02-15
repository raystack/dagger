package io.odpf.dagger.core.processors.internal.processor.function.functions;

import io.odpf.dagger.core.processors.internal.processor.function.FunctionProcessor;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.common.RowManager;

import java.sql.Timestamp;
import java.io.Serializable;

public class CurrentTimestampFunction implements FunctionProcessor, Serializable {
    public static final String CURRENT_TIMESTAMP_FUNCTION_KEY = "CURRENT_TIMESTAMP";

    private InternalSourceConfig internalSourceConfig;
    private Configuration configuration;

    public CurrentTimestampFunction(InternalSourceConfig internalSourceConfig, Configuration configuration) {
        this.internalSourceConfig = internalSourceConfig;
        this.configuration = configuration;
    }

    @Override
    public boolean canProcess(String functionName) {
        return CURRENT_TIMESTAMP_FUNCTION_KEY.equals(functionName);
    }

    @Override
    public Object getResult(RowManager rowManager) {
        return getCurrentTime();
    }

    /**
     * Gets current time.
     *
     * @return the current time
     */
    protected Timestamp getCurrentTime() {
        return new Timestamp(System.currentTimeMillis());
    }
}
