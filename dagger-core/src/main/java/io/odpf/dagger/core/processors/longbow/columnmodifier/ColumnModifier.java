package io.odpf.dagger.core.processors.longbow.columnmodifier;

/**
 * The interface Column modifier.
 */
public interface ColumnModifier {
    /**
     * Modify the column names.
     *
     * @param inputColumnNames the input column names
     * @return modified column names
     */
    String[] modifyColumnNames(String[] inputColumnNames);
}
