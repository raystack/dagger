package com.gojek.daggers.processors.longbow.columnmodifier;

public interface ColumnModifier {
    String[] modifyColumnNames(String[] inputColumnNames);
}
