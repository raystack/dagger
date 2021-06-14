package io.odpf.dagger.core.processors.internal.processor.sql.fields;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlInternalFieldConfig;

/**
 * The Sql internal auto field import.
 * used to get all the input columns and values to the output of internal post processor.
 */
public class SqlInternalAutoFieldImport implements SqlInternalFieldConfig {

    private ColumnNameManager columnNameManager;

    /**
     * Instantiates a new Sql internal auto field import.
     *
     * @param columnNameManager the column name manager
     */
    public SqlInternalAutoFieldImport(ColumnNameManager columnNameManager) {
        this.columnNameManager = columnNameManager;
    }

    @Override
    public void processInputColumns(RowManager rowManager) {
        for (String columnName : columnNameManager.getInputColumnNames()) {
            int inputFieldIndex = columnNameManager.getInputIndex(columnName);
            rowManager.setInOutput(columnNameManager.getOutputIndex(columnName), rowManager.getFromInput(inputFieldIndex));
        }
    }
}
