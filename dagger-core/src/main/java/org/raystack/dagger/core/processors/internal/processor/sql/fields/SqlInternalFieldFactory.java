package org.raystack.dagger.core.processors.internal.processor.sql.fields;

import org.raystack.dagger.core.processors.ColumnNameManager;
import org.raystack.dagger.core.utils.Constants;
import org.raystack.dagger.core.processors.internal.InternalSourceConfig;
import org.raystack.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import org.raystack.dagger.core.processors.internal.processor.sql.SqlInternalFieldConfig;

/**
 * The factory class for Sql internal field processor.
 */
public class SqlInternalFieldFactory {
    private ColumnNameManager columnNameManager;
    private SqlConfigTypePathParser sqlPathParser;
    private InternalSourceConfig internalSourceConfig;

    /**
     * Instantiates a new Sql internal field factory.
     *
     * @param columnNameManager    the column name manager
     * @param sqlPathParser        the sql path parser
     * @param internalSourceConfig the internal source config
     */
    public SqlInternalFieldFactory(ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser, InternalSourceConfig internalSourceConfig) {
        this.columnNameManager = columnNameManager;
        this.sqlPathParser = sqlPathParser;
        this.internalSourceConfig = internalSourceConfig;
    }

    /**
     * Gets sql internal field config.
     *
     * @return the sql internal field config
     */
    public SqlInternalFieldConfig getSqlInternalFieldConfig() {
        if (selectAllFromInputColumns()) {
            return new SqlInternalAutoFieldImport(columnNameManager);
        } else {
            return new SqlInternalFieldImport(columnNameManager, sqlPathParser, internalSourceConfig);
        }
    }

    private boolean selectAllFromInputColumns() {
        return Constants.SQL_PATH_SELECT_ALL_CONFIG_VALUE.equals(internalSourceConfig.getOutputField());
    }
}
