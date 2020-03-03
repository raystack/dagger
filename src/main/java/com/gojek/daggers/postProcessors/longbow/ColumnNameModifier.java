package com.gojek.daggers.postProcessors.longbow;

public interface ColumnNameModifier {
    String[] modifyColumnNames(String[] inputColumnNames);
}
