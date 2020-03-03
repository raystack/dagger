package com.gojek.daggers.postProcessors.longbow;

public class NoOpColumnModifier implements ColumnNameModifier {
    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        return inputColumnNames;
    }
}
