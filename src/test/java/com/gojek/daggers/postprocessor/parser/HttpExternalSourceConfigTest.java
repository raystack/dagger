package com.gojek.daggers.postprocessor.parser;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;

public class HttpExternalSourceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HttpExternalSourceConfig httpExternalSourceConfig;
    private HashMap<String, OutputMapping> outputMappings;
    private OutputMapping outputMapping;
    HashMap<String, String> headerMap;

    @Before
    public void setup() {
        headerMap = new HashMap<>();
        headerMap.put("content-type", "application/json");
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$.surge");
        outputMappings.put("surge_factor", outputMapping);
        httpExternalSourceConfig = new HttpExternalSourceConfig("http://localhost",
                "post", "request_body", "4000", "1000", false, headerMap, outputMappings);
    }

    @Test
    public void shouldReturnConnectTimeout() throws Exception {
        Assert.assertEquals("1000", httpExternalSourceConfig.getConnectTimeout());
    }

    @Test
    public void shouldReturnEndpoint() throws Exception {
        Assert.assertEquals("http://localhost", httpExternalSourceConfig.getEndpoint());
    }

    @Test
    public void shouldReturnStreamTimeout() throws Exception {
        Assert.assertEquals("4000", httpExternalSourceConfig.getStreamTimeout());
    }

    @Test
    public void shouldReturnBodyField() throws Exception {
        Assert.assertEquals("request_body", httpExternalSourceConfig.getBodyField());
    }

    @Test
    public void shouldReturnFailOnErrors() throws Exception {
        Assert.assertEquals(false, httpExternalSourceConfig.isFailOnErrors());
    }

    @Test
    public void shouldReturnVerb() throws Exception {
        Assert.assertEquals("post", httpExternalSourceConfig.getVerb());
    }

    @Test
    public void shouldReturnHeaderMap() throws Exception {
        Assert.assertEquals(headerMap, httpExternalSourceConfig.getHeaders());
    }

    @Test
    public void shouldReturnOutputMapping() throws Exception {
        Assert.assertEquals(outputMappings, httpExternalSourceConfig.getOutputMapping());
    }

    @Test
    public void shouldReturnColumnNames() {
        List<String> actualColumns = httpExternalSourceConfig.getColumns();
        String[] expectedColumns = {"surge_factor"};
        Assert.assertArrayEquals(expectedColumns, actualColumns.toArray());
    }

    @Test(expected = Test.None.class)
    public void shouldValidate() {
        httpExternalSourceConfig.validate();
    }

    @Test
    public void shouldThrowExceptionIfAllFieldsMissing() {
        expectedException.expectMessage("Missing required fields: [endpoint, bodyField, streamTimeout, connectTimeout, outputMapping]");
        expectedException.expect(IllegalArgumentException.class);

        HttpExternalSourceConfig httpExternalSourceConfig = new HttpExternalSourceConfig(null, null, null, null, null, false, null, null);
        httpExternalSourceConfig.validate();
    }

    @Test
    public void shouldThrowExceptionIfSomeFieldsMissing() {
        expectedException.expectMessage("Missing required fields: [streamTimeout, connectTimeout, outputMapping]");
        expectedException.expect(IllegalArgumentException.class);

        HttpExternalSourceConfig httpExternalSourceConfig = new HttpExternalSourceConfig("localhost", "post", "body", null, null, false, null, null);
        httpExternalSourceConfig.validate();
    }

    @Test
    public void shouldThrowExceptionIfFieldsOfNestedObjectsAreMissing() {
        expectedException.expectMessage("Missing required fields: [path]");
        expectedException.expect(IllegalArgumentException.class);

        OutputMapping outputMappingWithNullField = new OutputMapping(null);

        outputMappings.put("field", outputMappingWithNullField);

        httpExternalSourceConfig = new HttpExternalSourceConfig("http://localhost",
                "post", "request_body", "4000", "1000", false, headerMap, outputMappings);
        httpExternalSourceConfig.validate();
    }
}