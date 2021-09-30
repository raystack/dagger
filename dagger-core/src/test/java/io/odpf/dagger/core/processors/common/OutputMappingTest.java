package io.odpf.dagger.core.processors.common;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class OutputMappingTest {

    @Test
    public void shouldReturnPath() {
        OutputMapping outputMapping = new OutputMapping("path");
        String path = outputMapping.getPath();
        assertEquals("path", path);
    }

    @Test
    public void shouldValidate() {
        OutputMapping outputMapping = new OutputMapping("path");
        outputMapping.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfValidationFailed() {
        OutputMapping outputMapping = new OutputMapping(null);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> outputMapping.validateFields());
        assertEquals("Missing required fields: [path]", exception.getMessage());
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
