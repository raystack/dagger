package io.odpf.dagger.core.processors.internal;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.types.MapDecorator;
import io.odpf.dagger.core.processors.internal.processor.InternalConfigProcessor;

import org.apache.flink.types.Row;

/**
 * The decorator for Internal post processor.
 */
public class InternalDecorator implements MapDecorator {

    public static final int OUTPUT_ROW_INDEX = 1;

    private InternalSourceConfig internalSourceConfig;
    private InternalConfigProcessor internalConfigProcessor;
    private ColumnNameManager columnNameManager;

    /**
     * Instantiates a new Internal decorator.
     *
     * @param internalSourceConfig    the internal source config
     * @param internalConfigProcessor the internal config processor
     * @param columnNameManager       the column name manager
     */
    public InternalDecorator(InternalSourceConfig internalSourceConfig, InternalConfigProcessor internalConfigProcessor, ColumnNameManager columnNameManager) {
        this.internalSourceConfig = internalSourceConfig;
        this.internalConfigProcessor = internalConfigProcessor;
        this.columnNameManager = columnNameManager;
    }

    @Override
    public Boolean canDecorate() {
        return internalSourceConfig != null;
    }

    @Override
    public Row map(Row input) {
        Row outputRow = (Row) input.getField(OUTPUT_ROW_INDEX);
        if (outputColumnSizeIsDifferent(outputRow)) {
            input.setField(OUTPUT_ROW_INDEX, new Row(columnNameManager.getOutputSize()));
        }
        RowManager rowManager = new RowManager(input);
        internalConfigProcessor.process(rowManager);
        return rowManager.getAll();
    }

    private boolean outputColumnSizeIsDifferent(Row outputRow) {
        return outputRow != null && outputRow.getArity() != columnNameManager.getOutputSize();
    }
}
