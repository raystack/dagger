package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.core.exception.InputOutputMappingException;

import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * A class that responsible for managing input and output Row.
 */
public class RowManager {
    public static final int INPUT_ROW_INDEX = 0;
    public static final int OUTPUT_ROW_INDEX = 1;
    private Row parentRow;

    /**
     * Instantiates a new Row manager.
     *
     * @param row the row
     */
    public RowManager(Row row) {
        this.parentRow = row;
    }

    /**
     * Instantiates a new Row manager with specified input row and output row size.
     *
     * @param inputRow      the input row
     * @param outputRowSize the output row size
     */
    public RowManager(Row inputRow, int outputRowSize) {
        Row inputOutputRow = new Row(2);
        Row outputRow = new Row(outputRowSize);
        inputOutputRow.setField(INPUT_ROW_INDEX, inputRow);
        inputOutputRow.setField(OUTPUT_ROW_INDEX, outputRow);
        this.parentRow = inputOutputRow;
    }

    /**
     * Set value in output row.
     *
     * @param fieldIndex the field index
     * @param value      the value
     */
    public void setInOutput(int fieldIndex, Object value) {
        getChildRow(OUTPUT_ROW_INDEX).setField(fieldIndex, value);
    }

    /**
     * Get value from input row.
     *
     * @param fieldIndex the field index
     * @return the from input
     */
    public Object getFromInput(int fieldIndex) {
        return getChildRow(INPUT_ROW_INDEX).getField(fieldIndex);
    }

    private Row getChildRow(int index) {
        if (parentRow.getArity() != 2) {
            throw new InputOutputMappingException("InputOutputRow does not contain output. Something went wrong. Row Arity: " + parentRow.getArity());
        }
        return (Row) parentRow.getField(index);
    }

    /**
     * Gets all row value from parent row.
     *
     * @return the all
     */
    public Row getAll() {
        return parentRow;
    }

    /**
     * Gets input data.
     *
     * @return the input data
     */
    public Row getInputData() {
        return getChildRow(INPUT_ROW_INDEX);
    }

    /**
     * Gets output data.
     *
     * @return the output data
     */
    public Row getOutputData() {
        return getChildRow(OUTPUT_ROW_INDEX);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowManager that = (RowManager) o;
        return Objects.equals(parentRow, that.parentRow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentRow);
    }
}
