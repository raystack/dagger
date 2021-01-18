package com.gojek.daggers.postProcessors.internal.processor.sql;

import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;

public class SqlInternalAutoFieldImport implements SqlInternalFieldConfig {

    private ColumnNameManager columnNameManager;

    public SqlInternalAutoFieldImport(ColumnNameManager columnNameManager) {
        this.columnNameManager = columnNameManager;
    }

    @Override
    public void processInputColumns(RowManager rowManager) {
        for (String columnName : columnNameManager.getOutputColumnNames()){
            int inputFieldIndex = columnNameManager.getInputIndex(columnName);
            rowManager.setInOutput(columnNameManager.getOutputIndex(columnName), rowManager.getFromInput(inputFieldIndex));
        }
    }
}
