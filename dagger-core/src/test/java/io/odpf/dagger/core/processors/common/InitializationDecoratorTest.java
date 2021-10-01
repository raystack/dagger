package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.core.processors.ColumnNameManager;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class InitializationDecoratorTest {

    @Test
    public void canDecorateShouldBeFalse() {
        InitializationDecorator initializationDecorator = new InitializationDecorator(new ColumnNameManager(new String[0], new ArrayList<>()));
        assertFalse(initializationDecorator.canDecorate());
    }

    @Test
    public void shouldMapInputAndEmptyOutputRowInStream() {
        InitializationDecorator initializationDecorator = new InitializationDecorator(new ColumnNameManager(new String[5], Arrays.asList("one", "two", "three")));

        Row inputData = new Row(5);
        Row actualRow = initializationDecorator.map(inputData);
        assertEquals(2, actualRow.getArity());
        assertEquals(inputData, actualRow.getField(0));
        Row outputRow = ((Row) actualRow.getField(1));
        assertEquals(3, outputRow.getArity());
    }
}
