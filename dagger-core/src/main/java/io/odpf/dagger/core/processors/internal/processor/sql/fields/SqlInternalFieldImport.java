package io.odpf.dagger.core.processors.internal.processor.sql.fields;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlInternalFieldConfig;

/**
 * The Sql internal field import.
 */
public class SqlInternalFieldImport implements SqlInternalFieldConfig {

    private ColumnNameManager columnNameManager;
    private SqlConfigTypePathParser sqlPathParser;
    private InternalSourceConfig internalSourceConfig;

    /**
     * Instantiates a new Sql internal field import.
     *
     * @param columnNameManager    the column name manager
     * @param sqlPathParser        the sql path parser
     * @param internalSourceConfig the internal source config
     */
    public SqlInternalFieldImport(ColumnNameManager columnNameManager, SqlConfigTypePathParser sqlPathParser, InternalSourceConfig internalSourceConfig) {
        this.columnNameManager = columnNameManager;
        this.sqlPathParser = sqlPathParser;
        this.internalSourceConfig = internalSourceConfig;
    }

    @Override
    public void processInputColumns(RowManager rowManager) {
        int outputFieldIndex = columnNameManager.getOutputIndex(internalSourceConfig.getOutputField());
        if (outputFieldIndex != -1) {
            Object inputData = sqlPathParser.getData(rowManager);
            rowManager.setInOutput(outputFieldIndex, inputData);
        }
    }
}
