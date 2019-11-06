package com.gojek.daggers.postProcessors.external.common;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FetchOutputDecoratorTest {

    @Test
    public void canDecorateShouldBeFalse() {
        Assert.assertFalse(new FetchOutputDecorator().canDecorate());
    }

    @Test
    public void shouldMapOutputDataFromRowManager() {
        FetchOutputDecorator fetchOutputDecorator = new FetchOutputDecorator();
        Row parentRow = new Row(2);
        Row inputRow = new Row(3);
        Row outputRow = new Row(4);
        parentRow.setField(0, inputRow);
        parentRow.setField(1, outputRow);

        assertEquals(outputRow, fetchOutputDecorator.map(parentRow));
    }
}