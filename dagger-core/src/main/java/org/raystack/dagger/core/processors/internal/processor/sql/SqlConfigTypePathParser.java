package org.raystack.dagger.core.processors.internal.processor.sql;

import org.raystack.dagger.core.exception.InvalidConfigurationException;
import org.raystack.dagger.core.processors.ColumnNameManager;
import org.raystack.dagger.core.processors.common.RowManager;
import org.raystack.dagger.core.utils.Constants;
import org.raystack.dagger.core.processors.internal.InternalSourceConfig;

import java.io.Serializable;

/**
 * The Sql config type path parser.
 */
public class SqlConfigTypePathParser implements Serializable {

    private InternalSourceConfig internalSourceConfig;
    private ColumnNameManager columnNameManager;

    /**
     * Instantiates a new Sql config type path parser.
     *
     * @param internalSourceConfig2 the internal source config 2
     * @param columnNameManager     the column name manager
     */
    public SqlConfigTypePathParser(InternalSourceConfig internalSourceConfig2, ColumnNameManager columnNameManager) {
        this.internalSourceConfig = internalSourceConfig2;
        this.columnNameManager = columnNameManager;
    }

    /**
     * Gets data.
     *
     * @param rowManager the row manager
     * @return the data
     */
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
