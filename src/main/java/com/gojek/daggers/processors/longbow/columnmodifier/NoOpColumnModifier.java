package com.gojek.daggers.processors.longbow.columnmodifier;

public class NoOpColumnModifier implements ColumnModifier {
    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        return inputColumnNames;
    }
}
