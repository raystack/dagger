package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.core.exception.InputOutputMappingException;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.*;

public class RowManagerTest {


    @Test
    public void shouldSetInOutputRow() {
        Row outputRow = new Row(4);
        Row parentRow = Row.of(new Row(3), outputRow);
        RowManager rowManager = new RowManager(parentRow);

        rowManager.setInOutput(0, "one");

        assertEquals("one", outputRow.getField(0));
    }

    @Test
    public void shouldCreateParentRowWithInputDataAndOutputDimension() {
         Row inputRow = new Row(3);
        RowManager rowManager = new RowManager(inputRow, 4);

        Row outputData = rowManager.getOutputData();

        assertEquals(4, outputData.getArity());
        assertEquals(inputRow, rowManager.getInputData());
    }


    @Test
    public void shouldGetDataFromInputIndex() {
        Row inptRow = Row.of("other_input_data", "input_data");
        Row parentRow = Row.of(inptRow, new Row(4));
        RowManager rowManager = new RowManager(parentRow);

        assertEquals("input_data", rowManager.getFromInput(1));
    }

    @Test
    public void shouldGetParentRow() {
        Row parentRow = Row.of(new Row(3), new Row(4));
        RowManager rowManager = new RowManager(parentRow);

        Row expected = Row.of(new Row(3), new Row(4));
        assertEquals(expected, rowManager.getAll());
    }

    @Test
    public void shouldGetInputData() {
        Row parentRow = Row.of(new Row(3), new Row(4));
        RowManager rowManager = new RowManager(parentRow);

        assertEquals(new Row(3), rowManager.getInputData());
    }

    @Test
    public void shouldGetOutputData() {
        Row parentRow = Row.of(new Row(3), new Row(4));
        RowManager rowManager = new RowManager(parentRow);

        assertEquals(new Row(4), rowManager.getOutputData());
    }

    @Test
    public void shouldThrowExpectionOnGetOutputDataIfParentRowIsOfArityOtherThanTwo() {
        RowManager rowManager = new RowManager(new Row(3));
        assertThrows(InputOutputMappingException.class, () -> rowManager.getOutputData());
    }
}
