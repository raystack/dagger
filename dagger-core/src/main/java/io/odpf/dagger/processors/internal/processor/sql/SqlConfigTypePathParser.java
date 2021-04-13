package io.odpf.dagger.processors.internal.processor.sql;

import io.odpf.dagger.exception.InvalidConfigurationException;
import io.odpf.dagger.processors.ColumnNameManager;
import io.odpf.dagger.processors.common.RowManager;
import io.odpf.dagger.processors.internal.InternalSourceConfig;
import io.odpf.dagger.utils.Constants;

import java.io.Serializable;

public class SqlConfigTypePathParser implements Serializable {

    private InternalSourceConfig internalSourceConfig;
    private ColumnNameManager columnNameManager;

    public SqlConfigTypePathParser(InternalSourceConfig internalSourceConfig2, ColumnNameManager columnNameManager) {
        this.internalSourceConfig = internalSourceConfig2;
        this.columnNameManager = columnNameManager;
    }

    public Object getData(RowManager rowManager) {
        String inputField = internalSourceConfig.getValue();
        if (Constants.SQL_PATH_SELECT_ALL_CONFIG_VALUE.equals(inputField)) {
            return rowManager.getInputData();
        }
        int inputFieldIndex = columnNameManager.getInputIndex(inputField);
        if (inputFieldIndex == -1) {
            throw new InvalidConfigurationException(String.format("Value '%s' in input field for sql is wrongly configured", inputField));
        }
        return rowManager.getFromInput(inputFieldIndex);
    }
}
