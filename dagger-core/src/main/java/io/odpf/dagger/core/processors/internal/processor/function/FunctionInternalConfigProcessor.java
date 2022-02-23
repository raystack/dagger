package io.odpf.dagger.core.processors.internal.processor.function;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.InternalConfigProcessor;

import java.io.Serializable;

/**
 * The Function internal config processor.
 */
public class FunctionInternalConfigProcessor implements InternalConfigProcessor, Serializable {
    public static final String FUNCTION_CONFIG_HANDLER_TYPE = "function";

    private ColumnNameManager columnNameManager;
    private InternalSourceConfig internalSourceConfig;
    protected FunctionProcessor functionProcessor;

    /**
     * Instantiates a new Function internal config processor.
     *
     * @param columnNameManager    the column name manager
     * @param internalSourceConfig the internal source config
     * @param schemaConfig         the schema config
     */
    public FunctionInternalConfigProcessor(ColumnNameManager columnNameManager, InternalSourceConfig internalSourceConfig, SchemaConfig schemaConfig) {
        this.columnNameManager = columnNameManager;
        this.internalSourceConfig = internalSourceConfig;
        this.functionProcessor = FunctionProcessorFactory.getFunctionProcessor(internalSourceConfig, schemaConfig);
    }

    @Override
    public boolean canProcess(String type) {
        return FUNCTION_CONFIG_HANDLER_TYPE.equals(type);
    }

    @Override
    public void process(RowManager rowManager) {
        int outputFieldIndex = columnNameManager.getOutputIndex(internalSourceConfig.getOutputField());
        if (outputFieldIndex != -1) {
            rowManager.setInOutput(outputFieldIndex, functionProcessor.getResult(rowManager));
        }
    }
}
