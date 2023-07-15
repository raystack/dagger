package org.raystack.dagger.core.processors.internal.processor.sql.fields;

import org.raystack.dagger.core.processors.ColumnNameManager;
import org.raystack.dagger.core.processors.internal.InternalSourceConfig;
import org.raystack.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import org.raystack.dagger.core.processors.internal.processor.sql.SqlInternalFieldConfig;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class SqlInternalFieldFactoryTest {

    @Test
    public void shouldReturnSqlInternalAutoFieldImportClass() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("*", "*", "sql", null);
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);

        SqlInternalFieldFactory sqlInternalFieldFactory = new SqlInternalFieldFactory(columnNameManager, sqlPathParser, internalSourceConfig);
        SqlInternalFieldConfig sqlInternalFieldConfig = sqlInternalFieldFactory.getSqlInternalFieldConfig();

        assertEquals(SqlInternalAutoFieldImport.class, sqlInternalFieldConfig.getClass());
    }

    @Test
    public void shouldReturnSqlInternalFieldImportClass() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output_field", "value", "sql", null);
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);

        SqlInternalFieldFactory sqlInternalFieldFactory = new SqlInternalFieldFactory(columnNameManager, sqlPathParser, internalSourceConfig);
        SqlInternalFieldConfig sqlInternalFieldConfig = sqlInternalFieldFactory.getSqlInternalFieldConfig();

        assertEquals(SqlInternalFieldImport.class, sqlInternalFieldConfig.getClass());
    }
}
