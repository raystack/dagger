package io.odpf.dagger.core.processors.internal.processor.constant;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.InternalConfigProcessor;

import java.io.Serializable;

/**
 * The Constant internal config processor.
 */
public class ConstantInternalConfigProcessor implements InternalConfigProcessor, Serializable {
    public static final String CONSTANT_CONFIG_HANDLER_TYPE = "constant";
    private ColumnNameManager columnNameManager;
    private InternalSourceConfig internalSourceConfig;

    /**
     * Instantiates a new Constant internal config processor.
     *
     * @param columnNameManager    the column name manager
     * @param internalSourceConfig the internal source config
     */
    public ConstantInternalConfigProcessor(ColumnNameManager columnNameManager, InternalSourceConfig internalSourceConfig) {
        this.columnNameManager = columnNameManager;
        this.internalSourceConfig = internalSourceConfig;
    }

    @Override
    public boolean canProcess(String type) {
        return CONSTANT_CONFIG_HANDLER_TYPE.equals(type);
    }

    @Override
    public void process(RowManager rowManager) {
        int outputFieldIndex = columnNameManager.getOutputIndex(internalSourceConfig.getOutputField());
        if (outputFieldIndex != -1) {
            rowManager.setInOutput(outputFieldIndex, internalSourceConfig.getValue());
        }
    }
}
