package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.core.exception.InputOutputMappingException;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RowManagerTest {

    private Row parentRow;
    private Row inputRow;
    private Row outputRow;

    @Before
    public void setup() {
        parentRow = new Row(2);
        inputRow = new Row(3);
        outputRow = new Row(4);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);
    }

    @Test
    public void shouldSetInOutputRow() {
        RowManager rowManager = new RowManager(parentRow);

        rowManager.setInOutput(0, "one");

        Assert.assertEquals("one", outputRow.getField(0));
    }

    @Test
    public void shouldCreateParentRowWithInputDataAndOutputDimension() {
        inputRow = new Row(3);
        RowManager rowManager = new RowManager(inputRow, 4);

        Row outputData = rowManager.getOutputData();

        Assert.assertEquals(4, outputData.getArity());
        Assert.assertEquals(inputRow, rowManager.getInputData());
    }


    @Test
    public void shouldGetDataFromInputIndex() {
        inputRow.setField(1, "input_data");
        inputRow.setField(0, "other_input_data");
        RowManager rowManager = new RowManager(parentRow);

        Assert.assertEquals("input_data", rowManager.getFromInput(1));
    }

    @Test
    public void shouldGetParentRow() {
        RowManager rowManager = new RowManager(parentRow);

        Assert.assertEquals(parentRow, rowManager.getAll());
    }

    @Test
    public void shouldGetInputData() {
        RowManager rowManager = new RowManager(parentRow);

        Assert.assertEquals(inputRow, rowManager.getInputData());
    }

    @Test
    public void shouldGetOutputData() {
        RowManager rowManager = new RowManager(parentRow);

        Assert.assertEquals(outputRow, rowManager.getOutputData());
    }

    @Test(expected = InputOutputMappingException.class)
    public void shouldThrowExpectionOnGetOutputDataIfParentRowIsOfArityOtherThanTwo() {
        RowManager rowManager = new RowManager(new Row(3));
        rowManager.getOutputData();
    }
}
