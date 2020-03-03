package com.gojek.daggers.postProcessors.longbow.columnModifier;

public interface ColumnModifier {
    String[] modifyColumnNames(String[] inputColumnNames);
}
