package com.gojek.daggers.postProcessors.external.deprecated;

import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.HashMap;

import static org.junit.Assert.*;

public class TimestampDecoratorTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void canDecorateIsTrue() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "timestamp");
        TimestampDecorator timestampDecorator = new TimestampDecorator(configuration, 1);
        assertTrue(timestampDecorator.canDecorate());
    }

    @Test
    public void canDecorateIsFalse() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "es");
        TimestampDecorator timestampDecorator = new TimestampDecorator(configuration, 1);
        assertFalse(timestampDecorator.canDecorate());
    }

    @Test
    public void shouldMapToTimestampAtGivenIndex() {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put("source", "input");
        TimestampDecorator timestampDecorator = new TimestampDecorator(configuration, 0);
        Row row = new Row(1);
        Row result = timestampDecorator.map(row);
        assertEquals(result.getArity(), 1);
        assertTrue(result.getField(0) instanceof Timestamp);
    }
}