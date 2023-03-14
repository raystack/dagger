package com.gotocompany.dagger.core.processors.internal.processor.function.functions;

import com.gotocompany.dagger.core.processors.common.RowManager;
import com.gotocompany.dagger.core.processors.internal.processor.function.FunctionProcessor;

import java.sql.Timestamp;
import java.io.Serializable;
import java.time.Clock;

public class CurrentTimestampFunction implements FunctionProcessor, Serializable {
    public static final String CURRENT_TIMESTAMP_FUNCTION_KEY = "CURRENT_TIMESTAMP";

    private Clock clock;

    public CurrentTimestampFunction(Clock clock) {
        this.clock = clock;
    }
    @Override
    public boolean canProcess(String functionName) {
        return CURRENT_TIMESTAMP_FUNCTION_KEY.equals(functionName);
    }

    /**
     * Gets current time.
     *
     * @return the current time
     */
    @Override
    public Object getResult(RowManager rowManager) {
        return new Timestamp(clock.millis());
    }
}
