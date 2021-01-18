package com.gojek.daggers.postProcessors.internal.processor.sql;

import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Arrays;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SqlInternalConfigProcessorTest {

    @Mock
    private SqlConfigTypePathParser sqlConfigTypePathParser;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldBeAbleToProcessSqlCustomType(){
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "value", "sql");
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        Assert.assertTrue(sqlInternalConfigProcessor.canProcess(internalSourceConfig.getType()));
    }

    @Test
    public void shouldNotBeAbleToProcessFunctionCustomType(){
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "value", "function");
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        Assert.assertFalse(sqlInternalConfigProcessor.canProcess(internalSourceConfig.getType()));
    }

    @Test
    public void shouldNotBeAbleToProcessConstantCustomType(){
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{}, Arrays.asList());
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("field", "value", "constant");
        SqlConfigTypePathParser sqlPathParser = new SqlConfigTypePathParser(internalSourceConfig, columnNameManager);
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlPathParser, internalSourceConfig);

        Assert.assertFalse(sqlInternalConfigProcessor.canProcess(internalSourceConfig.getType()));
    }

    @Test
    public void shouldProcessToPopulateDataAtRightIndexForRightConfiguration(){
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"output_field"}, Arrays.asList("output1", "output_field", "output2"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output_field", "value", "sql");
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlConfigTypePathParser, internalSourceConfig);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        when(sqlConfigTypePathParser.getData(rowManager)).thenReturn("inputData");
        sqlInternalConfigProcessor.process(rowManager);

        Assert.assertEquals("inputData", rowManager.getOutputData().getField(1));
    }

    @Test
    public void shouldReturnAllOfTheInputFieldToOutputField(){
        ArrayList<String> outputColumnNames = new ArrayList<>();
        outputColumnNames.add("*");
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"inputField1", "inputField2"}, outputColumnNames);
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("*", "*", "sql");
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlConfigTypePathParser, internalSourceConfig);

        Row inputRow = new Row(2);
        inputRow.setField(0,"inputValue1");
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
    public void shouldNotReturnAllOfTheInputFieldToOutputField(){
        ArrayList<String> outputColumnNames = new ArrayList<>();
        outputColumnNames.add(".");
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"inputField1", "inputField2"}, outputColumnNames);
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig(".", "*", "sql");
        SqlInternalConfigProcessor sqlInternalConfigProcessor = new SqlInternalConfigProcessor(columnNameManager, sqlConfigTypePathParser, internalSourceConfig);

        Row inputRow = new Row(2);
        inputRow.setField(0,"inputValue1");
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
