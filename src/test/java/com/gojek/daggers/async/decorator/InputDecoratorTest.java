package com.gojek.daggers.async.decorator;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class InputDecoratorTest {

    @Test
    public void canDecorateIsTrue() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        InputDecorator inputDecorator = new InputDecorator(configuration, 0, 3);
        assertTrue(inputDecorator.canDecorate());
    }

    @Test
    public void canDecorateIsFalse() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "es");
        InputDecorator inputDecorator = new InputDecorator(configuration, 0, 3);
        assertFalse(inputDecorator.canDecorate());
    }

    @Test
    public void shouldMapFieldsAtGivenPositionInsideRow() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        InputDecorator inputDecorator = new InputDecorator(configuration, 0, 3);
        Row row = new Row(1);
        row.setField(0, "Test");
        Row result = inputDecorator.map(row);
        assertEquals(result.getArity(), 3);
        assertEquals(result.getField(0), row);
    }
}