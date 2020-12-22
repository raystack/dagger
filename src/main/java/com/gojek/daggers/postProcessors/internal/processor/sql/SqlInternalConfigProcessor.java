package com.gojek.daggers.postProcessors.internal.processor.sql;

import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;
import com.gojek.daggers.postProcessors.internal.processor.InternalConfigProcessor;

import java.io.Serializable;

import static com.gojek.daggers.utils.Constants.SQL_PATH_SELECT_ALL_CONFIG_VALUE;

public class SqlInternalConfigProcessor implements InternalConfigProcessor, Serializable {

    public static final String SQL_CONFIG_HANDLER_TYPE = "sql";

    private ColumnNameManager columnNameManager;
    private SqlConfigTypePathParser sqlPathParser;
    private InternalSourceConfig internalSourceConfig;

    public SqlInternalConfigProcessor(ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser, InternalSourceConfig internalSourceConfig) {
        this.columnNameManager = columnNameManager;
        this.sqlPathParser = sqlPathParser;
        this.internalSourceConfig = internalSourceConfig;
    }

    @Override
    public boolean canProcess(String type) {
        return SQL_CONFIG_HANDLER_TYPE.equals(type);
    }

    @Override
    public void process(RowManager rowManager) {
        if (selectAllFromInputColumns()) {
            for (String columnName : columnNameManager.getOutputColumnNames()){
                int inputFieldIndex = columnNameManager.getInputIndex(columnName);
                rowManager.setInOutput(columnNameManager.getOutputIndex(columnName), rowManager.getFromInput(inputFieldIndex));
            }
        } else {
            int outputFieldIndex = columnNameManager.getOutputIndex(internalSourceConfig.getOutputField());
            if (outputFieldIndex != -1) {
                Object inputData = sqlPathParser.getData(rowManager);
                rowManager.setInOutput(outputFieldIndex, inputData);
            }
        }
    }

    private boolean selectAllFromInputColumns() {
        return SQL_PATH_SELECT_ALL_CONFIG_VALUE.equals(internalSourceConfig.getOutputField());
    }
}
