package com.gojek.daggers.postProcessors.internal;

import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.MapDecorator;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.processor.InternalConfigProcessor;
import org.apache.flink.types.Row;

public class InternalDecorator implements MapDecorator {

    public static final int OUTPUT_ROW_INDEX = 1;

    private InternalSourceConfig internalSourceConfig;
    private InternalConfigProcessor internalConfigProcessor;
    private ColumnNameManager columnNameManager;


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
        if (outputRow != null && outputRow.getArity() != columnNameManager.getOutputSize()) {
            input.setField(OUTPUT_ROW_INDEX, new Row(columnNameManager.getOutputSize()));
        }
        RowManager rowManager = new RowManager(input);
        internalConfigProcessor.process(rowManager);
        return rowManager.getAll();
    }
}
