package com.gojek.daggers.postprocessor.parser;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class OutputMappingTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldReturnPath() {
        OutputMapping outputMapping = new OutputMapping("path");
        String path = outputMapping.getPath();
        assertEquals("path", path);
    }

    @Test(expected = Test.None.class)
    public void shouldValidate() {
        OutputMapping outputMapping = new OutputMapping("path");
        outputMapping.validate();
    }

    @Test
    public void shouldThrowExceptionIfValidationFailed() {
        expectedException.expectMessage("Missing required fields: [path]");
        expectedException.expect(IllegalArgumentException.class);
        OutputMapping outputMapping = new OutputMapping(null);
        outputMapping.validate();
    }

}