package com.gojek.daggers.postProcessors.external.deprecated;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class InputDecoratorDeprecatedTest {

    @Test
    public void canDecorateIsTrue() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        InputDecoratorDeprecated inputDecoratorDeprecated = new InputDecoratorDeprecated(configuration, 0, 3);
        assertTrue(inputDecoratorDeprecated.canDecorate());
    }

    @Test
    public void canDecorateIsFalse() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "es");
        InputDecoratorDeprecated inputDecoratorDeprecated = new InputDecoratorDeprecated(configuration, 0, 3);
        assertFalse(inputDecoratorDeprecated.canDecorate());
    }

    @Test
    public void shouldMapFieldsAtGivenPositionInsideRow() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        InputDecoratorDeprecated inputDecoratorDeprecated = new InputDecoratorDeprecated(configuration, 0, 3);
        Row row = new Row(1);
        row.setField(0, "Test");
        Row result = inputDecoratorDeprecated.map(row);
        assertEquals(result.getArity(), 3);
        assertEquals(result.getField(0), row);
    }
}