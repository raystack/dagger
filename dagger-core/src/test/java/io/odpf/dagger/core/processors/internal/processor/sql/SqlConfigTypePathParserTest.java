package io.odpf.dagger.core.processors.internal.processor.sql;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

public class SqlConfigTypePathParserTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldReturnAllOfTheInputData() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("all", "*", "sql");
        SqlConfigTypePathParser sqlConfigTypePathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);

        Row inputRow = new Row(2);
        inputRow.setField(0, "input1");
        inputRow.setField(1, "input2");
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        Object data = sqlConfigTypePathParser.getData(rowManager);

        Assert.assertEquals(rowManager.getInputData(), data);
    }

    @Test
    public void shouldThrowExceptionWhenTheColumnNameIsNotFoundInInputColumns() {
        expectedException.expect(InvalidConfigurationException.class);
        expectedException.expectMessage("Value 'value' in input field for sql is wrongly configured");

        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "value", "sql");
        SqlConfigTypePathParser sqlConfigTypePathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        sqlConfigTypePathParser.getData(rowManager);
    }

    @Test
    public void shouldReturnTheInputDataAsPerTheConfiguration() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "input2", "sql");
        SqlConfigTypePathParser sqlConfigTypePathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);

        Row inputRow = new Row(2);
        inputRow.setField(0, "inputData1");
        inputRow.setField(1, "inputData2");
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        Object data = sqlConfigTypePathParser.getData(rowManager);

        Assert.assertEquals("inputData2", data);
    }

}
