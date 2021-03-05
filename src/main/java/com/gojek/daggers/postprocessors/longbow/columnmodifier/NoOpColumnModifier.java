package com.gojek.daggers.postprocessors.longbow.columnmodifier;

public class NoOpColumnModifier implements ColumnModifier {
    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        return inputColumnNames;
    }
}
