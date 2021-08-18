package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.core.processors.ColumnNameManager;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class InitializationDecoratorTest {

    @Test
    public void canDecorateShouldBeFalse() {
        InitializationDecorator initializationDecorator = new InitializationDecorator(new ColumnNameManager(new String[0], new ArrayList<>()));
        //TODO use static import
        Assert.assertFalse(initializationDecorator.canDecorate());
    }

    @Test
    public void shouldMapInputAndEmptyOutputRowInStream() {
        InitializationDecorator initializationDecorator = new InitializationDecorator(new ColumnNameManager(new String[5], Arrays.asList("one", "two", "three")));

        Row inputData = new Row(5);
        Row actualRow = initializationDecorator.map(inputData);
        //TODO use static import
        Assert.assertEquals(2, actualRow.getArity());
        Assert.assertEquals(inputData, actualRow.getField(0));
        Row outputRow = ((Row) actualRow.getField(1));
        Assert.assertEquals(3, outputRow.getArity());
    }
}
