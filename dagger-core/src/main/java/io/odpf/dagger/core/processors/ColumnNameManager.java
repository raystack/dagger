package io.odpf.dagger.core.processors;

import io.odpf.dagger.core.utils.Constants;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Manage input and output column names.
 */
public class ColumnNameManager implements Serializable {
    private final List<String> inputColumnNames;
    private List<String> outputColumnNames;

    /**
     * Instantiates a new Column name manager.
     *
     * @param inputColumnNames  the input column names
     * @param outputColumnNames the output column names
     */
    public ColumnNameManager(String[] inputColumnNames, List<String> outputColumnNames) {
        this.inputColumnNames = Arrays.asList(inputColumnNames);
        this.outputColumnNames = setOutputColumnNames(outputColumnNames);
    }

    /**
     * Gets input columns index.
     *
     * @param inputColumnName the input column name
     * @return the input index
     */
    public Integer getInputIndex(String inputColumnName) {
        return inputColumnNames.indexOf(inputColumnName);
    }

    /**
     * Gets output columns index.
     *
     * @param outputColumnName the output column name
     * @return the output index
     */
    public Integer getOutputIndex(String outputColumnName) {
        return outputColumnNames.indexOf(outputColumnName);
    }

    /**
     * Gets output size.
     *
     * @return the output columns size
     */
    public int getOutputSize() {
        return outputColumnNames.size();
    }

    /**
     * Get output column names string [ ].
     *
     * @return the output column names
     */
    public String[] getOutputColumnNames() {
        return outputColumnNames.toArray(new String[0]);
    }

    /**
     * Get input column names string [ ].
     *
     * @return the input column names
     */
    public String[] getInputColumnNames() {
        return inputColumnNames.toArray(new String[0]);
    }

    private List<String> setOutputColumnNames(List<String> names) {
        if (selectAllFromInputColumns(names)) {
            names.remove(Constants.SQL_PATH_SELECT_ALL_CONFIG_VALUE);
            names.addAll(inputColumnNames);
        }
        return names;
    }

    private boolean selectAllFromInputColumns(List<String> names) {
        return names != null && names.contains(Constants.SQL_PATH_SELECT_ALL_CONFIG_VALUE);
    }
}
