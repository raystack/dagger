package io.odpf.dagger.core.processors.internal.processor.sql.fields;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SqlInternalFieldImportTest {

    @Mock
    private ColumnNameManager defaultColumnNameManager;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldProcessToPopulateDataAtRightIndexForRightConfiguration() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"inputField"}, Arrays.asList("output1", "outputField", "output2"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("outputField", "inputField", "sql", null);
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalFieldImport sqlInternalFieldImport = new SqlInternalFieldImport(columnNameManager, sqlPathParser, internalSourceConfig);

        Row inputRow = new Row(1);
        inputRow.setField(0, "inputValue1");
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        sqlInternalFieldImport.processInputColumns(rowManager);

        assertEquals("inputValue1", rowManager.getOutputData().getField(1));
    }

    @Test
    public void shouldReturnNullIfOutputIndexIsNotFoundInOutputColumnManager() {
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("outputField", "inputField", "sql", null);
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, defaultColumnNameManager);
        SqlInternalFieldImport sqlInternalFieldImport = new SqlInternalFieldImport(defaultColumnNameManager, sqlPathParser, internalSourceConfig);

        Row inputRow = new Row(1);
        inputRow.setField(0, "inputValue1");
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        when(defaultColumnNameManager.getOutputIndex("field")).thenReturn(-1);
        sqlInternalFieldImport.processInputColumns(rowManager);

        assertNull(rowManager.getOutputData().getField(1));
    }
}
