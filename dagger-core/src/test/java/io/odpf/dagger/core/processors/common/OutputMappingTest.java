package io.odpf.dagger.core.processors.common;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;

import static org.junit.Assert.assertArrayEquals;
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
        outputMapping.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfValidationFailed() {
        expectedException.expectMessage("Missing required fields: [path]");
        expectedException.expect(IllegalArgumentException.class);
        OutputMapping outputMapping = new OutputMapping(null);
        outputMapping.validateFields();
    }

    @Test
    public void shouldReturnMandatoryFields() {
        OutputMapping outputMapping = new OutputMapping("$.surge");
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();
        expectedMandatoryFields.put("path", "$.surge");
        HashMap<String, Object> actualMandatoryFields = outputMapping.getMandatoryFields();
        assertArrayEquals(expectedMandatoryFields.values().toArray(), actualMandatoryFields.values().toArray());
    }

}
