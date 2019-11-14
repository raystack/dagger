package com.gojek.daggers.postProcessors.internal.processor.sql;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;

import java.io.Serializable;

import static com.gojek.daggers.utils.Constants.SQL_PATH_SELECT_ALL_CONFIG_VALUE;

public class SqlConfigTypePathParser implements Serializable {

    private InternalSourceConfig internalSourceConfig;
    private ColumnNameManager columnNameManager;

    public SqlConfigTypePathParser(InternalSourceConfig internalSourceConfig2, ColumnNameManager columnNameManager) {
        this.internalSourceConfig = internalSourceConfig2;
        this.columnNameManager = columnNameManager;
    }

    public Object getData(RowManager rowManager) {
        String inputField = internalSourceConfig.getValue();
        if (SQL_PATH_SELECT_ALL_CONFIG_VALUE.equals(inputField)) {
            return rowManager.getInputData();
        }
        int inputFieldIndex = columnNameManager.getInputIndex(inputField);
        if (inputFieldIndex == -1)
            throw new InvalidConfigurationException(String.format("Value '%s' in input field for sql is wrongly configured",inputField));
        return rowManager.getFromInput(inputFieldIndex);
    }

}
