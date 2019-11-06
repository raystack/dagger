package com.gojek.daggers.postProcessors.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ColumnNameManager implements Serializable {
    private final List<String> inputColumnNames;
    private final List<String> outputColumnNames;

    public ColumnNameManager(String[] inputColumnNames, List<String> outputColumnNames) {
        this.inputColumnNames = Arrays.asList(inputColumnNames);
        this.outputColumnNames = outputColumnNames;
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
}
