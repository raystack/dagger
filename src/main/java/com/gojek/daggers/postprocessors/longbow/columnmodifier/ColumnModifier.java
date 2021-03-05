package com.gojek.daggers.postprocessors.longbow.columnmodifier;

public interface ColumnModifier {
    String[] modifyColumnNames(String[] inputColumnNames);
}
