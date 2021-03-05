package com.gojek.daggers.postprocessors.internal.processor.sql.fields;

import com.gojek.daggers.postprocessors.common.ColumnNameManager;
import com.gojek.daggers.postprocessors.external.common.RowManager;
import com.gojek.daggers.postprocessors.internal.processor.sql.SqlInternalFieldConfig;

public class SqlInternalAutoFieldImport implements SqlInternalFieldConfig {

    private ColumnNameManager columnNameManager;

    public SqlInternalAutoFieldImport(ColumnNameManager columnNameManager) {
        this.columnNameManager = columnNameManager;
    }

    @Override
    public void processInputColumns(RowManager rowManager) {
        for (String columnName : columnNameManager.getInputColumnNames()){
            int inputFieldIndex = columnNameManager.getInputIndex(columnName);
            rowManager.setInOutput(columnNameManager.getOutputIndex(columnName), rowManager.getFromInput(inputFieldIndex));
        }
    }
}
