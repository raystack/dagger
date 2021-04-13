package io.odpf.dagger.processors.longbow.columnmodifier;

public interface ColumnModifier {
    String[] modifyColumnNames(String[] inputColumnNames);
}
