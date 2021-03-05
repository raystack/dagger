package com.gojek.daggers.postprocessors.internal.processor.sql.fields;

import com.gojek.daggers.postprocessors.common.ColumnNameManager;
import com.gojek.daggers.postprocessors.internal.InternalSourceConfig;
import com.gojek.daggers.postprocessors.internal.processor.sql.SqlConfigTypePathParser;
import com.gojek.daggers.postprocessors.internal.processor.sql.SqlInternalFieldConfig;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class SqlInternalFieldFactoryTest {

    @Test
    public void shouldReturnSqlInternalAutoFieldImportClass(){
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("*", "*", "sql");
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);

        SqlInternalFieldFactory sqlInternalFieldFactory = new SqlInternalFieldFactory(columnNameManager, sqlPathParser, internalSourceConfig);
        SqlInternalFieldConfig sqlInternalFieldConfig = sqlInternalFieldFactory.getSqlInternalFieldConfig();

        assertEquals(SqlInternalAutoFieldImport.class, sqlInternalFieldConfig.getClass());
    }

    @Test
    public void shouldReturnSqlInternalFieldImportClass(){
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output_field", "value", "sql");
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);

        SqlInternalFieldFactory sqlInternalFieldFactory = new SqlInternalFieldFactory(columnNameManager, sqlPathParser, internalSourceConfig);
        SqlInternalFieldConfig sqlInternalFieldConfig = sqlInternalFieldFactory.getSqlInternalFieldConfig();

        assertEquals(SqlInternalFieldImport.class, sqlInternalFieldConfig.getClass());
    }
}
