package io.odpf.dagger.core.processors.internal.processor.sql.fields;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class SqlInternalConfigProcessorTest {

    @Test
    public void shouldBeAbleToProcessSqlCustomType() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "value", "sql", null);
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        assertTrue(sqlInternalConfigProcessor.canProcess(internalSourceConfig.getType()));
    }

    @Test
    public void shouldNotBeAbleToProcessFunctionCustomType() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "value", "function", null);
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        assertFalse(sqlInternalConfigProcessor.canProcess(internalSourceConfig.getType()));
    }

    @Test
    public void shouldNotBeAbleToProcessConstantCustomType() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "value", "constant", null);
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        assertFalse(sqlInternalConfigProcessor.canProcess(internalSourceConfig.getType()));
    }

    @Test
    public void processWithRightConfiguration() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"field"}, Arrays.asList("field1", "newField", "field2"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("newField", "field", "sql", null);
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        sqlInternalConfigProcessor.process(rowManager);
    }
}
