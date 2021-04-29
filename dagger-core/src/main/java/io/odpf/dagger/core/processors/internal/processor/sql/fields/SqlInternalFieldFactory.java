package io.odpf.dagger.core.processors.internal.processor.sql.fields;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlInternalFieldConfig;

import static io.odpf.dagger.core.utils.Constants.SQL_PATH_SELECT_ALL_CONFIG_VALUE;

public class SqlInternalFieldFactory {
    private ColumnNameManager columnNameManager;
    private SqlConfigTypePathParser sqlPathParser;
    private InternalSourceConfig internalSourceConfig;

    public SqlInternalFieldFactory(ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser, InternalSourceConfig internalSourceConfig) {
        this.columnNameManager = columnNameManager;
        this.sqlPathParser = sqlPathParser;
        this.internalSourceConfig = internalSourceConfig;
    }

    public SqlInternalFieldConfig getSqlInternalFieldConfig() {
        if (selectAllFromInputColumns()) {
            return new SqlInternalAutoFieldImport(columnNameManager);
        } else {
            return new SqlInternalFieldImport(columnNameManager, sqlPathParser, internalSourceConfig);
        }
    }

    private boolean selectAllFromInputColumns() {
        return SQL_PATH_SELECT_ALL_CONFIG_VALUE.equals(internalSourceConfig.getOutputField());
    }
}
