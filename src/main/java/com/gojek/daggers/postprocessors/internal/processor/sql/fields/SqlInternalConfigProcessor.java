package com.gojek.daggers.postprocessors.internal.processor.sql.fields;

import com.gojek.daggers.postprocessors.common.ColumnNameManager;
import com.gojek.daggers.postprocessors.external.common.RowManager;
import com.gojek.daggers.postprocessors.internal.InternalSourceConfig;
import com.gojek.daggers.postprocessors.internal.processor.InternalConfigProcessor;
import com.gojek.daggers.postprocessors.internal.processor.sql.SqlConfigTypePathParser;
import com.gojek.daggers.postprocessors.internal.processor.sql.SqlInternalFieldConfig;

import java.io.Serializable;

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
        SqlInternalFieldConfig sqlInternalFieldConfig =
                new SqlInternalFieldFactory(columnNameManager, sqlPathParser, internalSourceConfig).getSqlInternalFieldConfig();
        sqlInternalFieldConfig.processInputColumns(rowManager);
    }
}
