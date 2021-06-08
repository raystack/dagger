package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.InternalConfigProcessor;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * The Function internal config processor.
 */
public class FunctionInternalConfigProcessor implements InternalConfigProcessor, Serializable {

    public static final String FUNCTION_CONFIG_HANDLER_TYPE = "function";
    public static final String CURRENT_TIMESTAMP_FUNCTION_KEY = "CURRENT_TIMESTAMP";

    private ColumnNameManager columnNameManager;
    private InternalSourceConfig internalSourceConfig;

    /**
     * Instantiates a new Function internal config processor.
     *
     * @param columnNameManager    the column name manager
     * @param internalSourceConfig the internal source config
     */
    public FunctionInternalConfigProcessor(ColumnNameManager columnNameManager, InternalSourceConfig internalSourceConfig) {
        this.columnNameManager = columnNameManager;
        this.internalSourceConfig = internalSourceConfig;
    }

    @Override
    public boolean canProcess(String type) {
        return FUNCTION_CONFIG_HANDLER_TYPE.equals(type);
    }

    @Override
    public void process(RowManager rowManager) {
        int outputFieldIndex = columnNameManager.getOutputIndex(internalSourceConfig.getOutputField());
        if (outputFieldIndex != -1) {
            if (!CURRENT_TIMESTAMP_FUNCTION_KEY.equals(internalSourceConfig.getValue())) {
                throw new InvalidConfigurationException(String.format("The function %s is not supported in custom configuration", internalSourceConfig.getValue()));
            }
            rowManager.setInOutput(outputFieldIndex, getCurrentTime());
        }
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
