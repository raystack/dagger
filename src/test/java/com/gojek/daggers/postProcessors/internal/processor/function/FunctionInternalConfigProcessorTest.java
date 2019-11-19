package com.gojek.daggers.postProcessors.internal.processor.function;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.internal.InternalSourceConfig;
import org.apache.flink.table.api.scala.currentTimestamp;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.shaded.org.apache.commons.net.ntp.TimeStamp;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class FunctionInternalConfigProcessorTest {

    @Test
    public void shouldBeAbleToProcessFunctionCustomType(){
        ColumnNameManager columnManager = new ColumnNameManager(new String[0], new ArrayList<>());
        FunctionInternalConfigProcessor functionInternalConfigProcessor= new FunctionInternalConfigProcessor(columnManager, getCustomConfig("function"));

        Assert.assertTrue(functionInternalConfigProcessor.canProcess("function"));
    }

    @Test
    public void shouldNotBeAbleToProcessConstantCustomType(){
        ColumnNameManager columnManager = new ColumnNameManager(new String[0], new ArrayList<>());
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(columnManager, getCustomConfig("constant"));

        Assert.assertFalse(functionInternalConfigProcessor.canProcess("constant"));
    }

    @Test
    public void shouldNotBeAbleToProcessSqlCustomType(){
        ColumnNameManager columnManager = new ColumnNameManager(new String[0], new ArrayList<>());
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(columnManager, getCustomConfig("sql"));

        Assert.assertFalse(functionInternalConfigProcessor.canProcess("sql"));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void shouldThrowInvalidConfigurationException(){
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output3", "test", "function");
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(columnNameManager, internalSourceConfig);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        functionInternalConfigProcessor.process(rowManager);
    }

    @Test
    public void shouldProcessToPopulateDataAtRightIndexForRightConfiguration(){
        ColumnNameManager columnNameManager = new ColumnNameManager(new String[]{"input1", "input2"}, Arrays.asList("output1", "output2", "output3"));
        InternalSourceConfig internalSourceConfig = new InternalSourceConfig("output2", "CURRENT_TIMESTAMP", "function");
        FunctionInternalConfigProcessor functionInternalConfigProcessor = new FunctionInternalConfigProcessor(columnNameManager, internalSourceConfig);

        Row inputRow = new Row(2);
        Row outputRow = new Row(3);
        Row parentRow = new Row(2);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
        RowManager rowManager = new RowManager(parentRow);

        functionInternalConfigProcessor.process(rowManager);

        Assert.assertNotNull(rowManager.getOutputData().getField(1));
    }

    private InternalSourceConfig getCustomConfig(String type) {
        return new InternalSourceConfig("field", "value", type);
    }
}
