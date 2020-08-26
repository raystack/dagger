package com.gojek.daggers.postProcessors.internal.processor.function;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;
import com.gojek.daggers.postProcessors.internal.processor.InternalConfigProcessor;

import java.io.Serializable;
import java.sql.Timestamp;

public class FunctionInternalConfigProcessor implements InternalConfigProcessor, Serializable {

    public static final String FUNCTION_CONFIG_HANDLER_TYPE = "function";
    public static final String CURRENT_TIMESTAMP_FUNCTION_KEY = "CURRENT_TIMESTAMP";

    private ColumnNameManager columnNameManager;
    private InternalSourceConfig internalSourceConfig;

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

    protected Timestamp getCurrentTime() {
        return new Timestamp(System.currentTimeMillis());
    }
}
