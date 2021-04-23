package io.odpf.dagger.core.processors.longbow.columnmodifier;

public class NoOpColumnModifier implements ColumnModifier {
    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        return inputColumnNames;
    }
}
