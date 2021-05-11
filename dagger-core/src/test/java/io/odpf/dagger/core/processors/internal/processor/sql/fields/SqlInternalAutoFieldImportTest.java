package io.odpf.dagger.core.processors.internal.processor.sql.fields;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import io.odpf.dagger.core.processors.internal.processor.sql.SqlConfigTypePathParser;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class SqlInternalAutoFieldImportTest {

    @Test
    public void shouldReturnAllOfTheInputFieldToOutputField() {
        ArrayList<String> outputColumnNames = new ArrayList<>();
        outputColumnNames.add("*");
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"inputField1", "inputField2"}, outputColumnNames);
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("*", "*", "sql");
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        Row inputRow = new Row(2);
        inputRow.setField(0, "inputValue1");
        inputRow.setField(1, "inputValue2");
        Row outputRow = new Row(2);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        sqlInternalConfigProcessor.process(rowManager);

        Assert.assertEquals(rowManager.getInputData(), rowManager.getOutputData());
    }

    @Test
    public void shouldReturnAllOfTheInputFieldToOutputFieldWithAnyOutputValueConfig() {
        ArrayList<String> outputColumnNames = new ArrayList<>();
        outputColumnNames.add("*");
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"inputField1", "inputField2"}, outputColumnNames);
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("*", "anyString", "sql");
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        Row inputRow = new Row(2);
        inputRow.setField(0, "inputValue1");
        inputRow.setField(1, "inputValue2");
        Row outputRow = new Row(2);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        sqlInternalConfigProcessor.process(rowManager);

        Assert.assertEquals(rowManager.getInputData(), rowManager.getOutputData());
    }

    @Test
    public void shouldNotReturnAllOfTheInputFieldToOutputField() {
        ArrayList<String> outputColumnNames = new ArrayList<>();
        outputColumnNames.add(".");
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"inputField1", "inputField2"}, outputColumnNames);
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig(".", "*", "sql");
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        Row inputRow = new Row(2);
        inputRow.setField(0, "inputValue1");
        inputRow.setField(1, "inputValue2");
        Row outputRow = new Row(2);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        sqlInternalConfigProcessor.process(rowManager);

        Assert.assertNotEquals(rowManager.getInputData(), rowManager.getOutputData());
    }
}
