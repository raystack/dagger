package com.gojek.daggers.processors.common;

import com.gojek.daggers.exception.InputOutputMappingException;
import org.apache.flink.types.Row;

import java.util.Objects;

public class RowManager {
    public static final int INPUT_ROW_INDEX = 0;
    public static final int OUTPUT_ROW_INDEX = 1;
    private Row parentRow;

    public RowManager(Row row) {
        this.parentRow = row;
    }

    public RowManager(Row inputRow, int outputRowSize) {
        Row inputOutputRow = new Row(2);
        Row outputRow = new Row(outputRowSize);
        inputOutputRow.setField(INPUT_ROW_INDEX, inputRow);
        inputOutputRow.setField(OUTPUT_ROW_INDEX, outputRow);
        this.parentRow = inputOutputRow;
    }

    public void setInOutput(int fieldIndex, Object value) {
        getChildRow(OUTPUT_ROW_INDEX).setField(fieldIndex, value);
    }

    public Object getFromInput(int fieldIndex) {
        return getChildRow(INPUT_ROW_INDEX).getField(fieldIndex);
    }

    private Row getChildRow(int index) {
        if (parentRow.getArity() != 2)
            throw new InputOutputMappingException("InputOutputRow does not contain output. Something went wrong. Row Arity: " + parentRow.getArity());
        return (Row) parentRow.getField(index);
    }

    public Row getAll() {
        return parentRow;
    }

    public Row getInputData() {
        return getChildRow(INPUT_ROW_INDEX);
    }

    public Row getOutputData() {
        return getChildRow(OUTPUT_ROW_INDEX);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowManager that = (RowManager) o;
        return Objects.equals(parentRow, that.parentRow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentRow);
    }
}
