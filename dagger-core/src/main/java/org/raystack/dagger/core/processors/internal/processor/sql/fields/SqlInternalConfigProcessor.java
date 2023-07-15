package org.raystack.dagger.core.processors.internal.processor.sql.fields;

import org.raystack.dagger.core.processors.ColumnNameManager;
import org.raystack.dagger.core.processors.common.RowManager;
import org.raystack.dagger.core.processors.internal.InternalSourceConfig;
import org.raystack.dagger.core.processors.internal.processor.InternalConfigProcessor;
import org.raystack.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import org.raystack.dagger.core.processors.internal.processor.sql.SqlInternalFieldConfig;

import java.io.Serializable;

/**
 * The Sql internal config processor.
 */
public class SqlInternalConfigProcessor implements InternalConfigProcessor, Serializable {

    public static final String SQL_CONFIG_HANDLER_TYPE = "sql";

    private ColumnNameManager columnNameManager;
    private SqlConfigTypePathParser sqlPathParser;
    private InternalSourceConfig internalSourceConfig;

    /**
     * Instantiates a new Sql internal config processor.
     *
     * @param columnNameManager    the column name manager
     * @param sqlPathParser        the sql path parser
     * @param internalSourceConfig the internal source config
     */
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
        SqlInternalFieldConfig sqlInternalFieldConfig =
                new SqlInternalFieldFactory(columnNameManager, sqlPathParser, internalSourceConfig).getSqlInternalFieldConfig();
        sqlInternalFieldConfig.processInputColumns(rowManager);
    }
}
