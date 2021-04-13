package io.odpf.dagger.processors.longbow.columnmodifier;

public class NoOpColumnModifier implements ColumnModifier {
    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        return inputColumnNames;
    }
}
