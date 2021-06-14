package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.types.MapDecorator;

import org.apache.flink.types.Row;

/**
 * The Initialization decorator.
 */
public class InitializationDecorator implements MapDecorator {

    private ColumnNameManager columnNameManager;

    /**
     * Instantiates a new Initialization decorator.
     *
     * @param columnNameManager the column name manager
     */
    public InitializationDecorator(ColumnNameManager columnNameManager) {
        this.columnNameManager = columnNameManager;
    }

    @Override
    public Boolean canDecorate() {
        return false;
    }

    @Override
    public Row map(Row input) {
        RowManager rowManager = new RowManager(input, columnNameManager.getOutputSize());
        return rowManager.getAll();
    }


}
