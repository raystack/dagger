package io.odpf.dagger.core.processors.longbow.columnmodifier;

/**
 * The No op column modifier.
 */
public class NoOpColumnModifier implements ColumnModifier {
    @Override
    public String[] modifyColumnNames(String[] inputColumnNames) {
        return inputColumnNames;
    }
}
