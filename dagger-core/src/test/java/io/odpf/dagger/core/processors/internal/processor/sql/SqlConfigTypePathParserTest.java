package io.odpf.dagger.core.processors.internal.processor.sql;

import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.internal.InternalSourceConfig;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class SqlConfigTypePathParserTest {

    @Test
    public void shouldReturnAllOfTheInputData() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("all", "*", "sql", null);
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
        assertEquals(Row.of("input1", "input2"), data);
    }

    @Test
    public void shouldThrowExceptionWhenTheColumnNameIsNotFoundInInputColumns() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "value", "sql", null);
        SqlConfigTypePathParser sqlConfigTypePathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class,
                () -> sqlConfigTypePathParser.getData(rowManager));
        assertEquals("Value 'value' in input field for sql is wrongly configured", exception.getMessage());
    }

    @Test
    public void shouldReturnTheInputDataAsPerTheConfiguration() {
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "input2", "sql", null);
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

        assertEquals("inputData2", data);
    }

}
