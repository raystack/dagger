package io.odpf.dagger.core.processors.internal.processor.function;
import io.odpf.dagger.core.processors.common.RowManager;

public interface FunctionProcessor {
    /**
     * Check if can process internal post processor.
     *
     * @param type the type
     * @return the boolean
     */
    boolean canProcess(String functionName);

    /**
     * Process.
     *
     * @param rowManager the row manager
     */
    Object getResult(RowManager rowManager);
}
