package com.gojek.daggers.postProcessors.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ColumnNameManager implements Serializable {
    private List<String> inputColumnNames;
    private List<String> outputColumnNames;

    public ColumnNameManager(String[] inputColumnNames, List<String> outputColumnNames) {
        this.inputColumnNames = Arrays.asList(inputColumnNames);
        this.outputColumnNames = setOutputColumnNames(outputColumnNames);
    }

    public Integer getInputIndex(String inputColumnName) {
        return inputColumnNames.indexOf(inputColumnName);
    }

    public Integer getOutputIndex(String outputColumnName) {
        return outputColumnNames.indexOf(outputColumnName);
    }

    public int getOutputSize() {
        return outputColumnNames.size();
    }

    public String[] getOutputColumnNames() {
        return outputColumnNames.toArray(new String[0]);
    }

    private List<String> setOutputColumnNames(List<String> outputColumnNames) {
        if (selectAllFromInputColumns(outputColumnNames)) {
            outputColumnNames.remove(SQL_PATH_SELECT_ALL_CONFIG_VALUE);
            outputColumnNames.addAll(inputColumnNames);
        }
        return outputColumnNames;
    }

    private boolean selectAllFromInputColumns(List<String> outputColumnNames) {
        return outputColumnNames != null && outputColumnNames.contains(SQL_PATH_SELECT_ALL_CONFIG_VALUE);
    }
}