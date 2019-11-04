package com.gojek.daggers.postprocessor.parser;

import com.gojek.daggers.postprocessor.configs.HttpExternalSourceConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
                "post", "request_body", "4000", "1000", false, "com.gojek.esb.booking.BookingLogMessage", headerMap, outputMappings);
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
        Assert.assertEquals("request_body", httpExternalSourceConfig.getBodyColumnFromSql());
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
    public void shouldReturnType() throws Exception {
        Assert.assertEquals("com.gojek.esb.booking.BookingLogMessage", httpExternalSourceConfig.getType());
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
        httpExternalSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfAllFieldsMissing() {
        expectedException.expect(IllegalArgumentException.class);

        HttpExternalSourceConfig httpExternalSourceConfig = new HttpExternalSourceConfig(null, null, null, null, null, false, null, null, null);
        httpExternalSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfSomeFieldsMissing() {
        expectedException.expectMessage("Missing required fields: [streamTimeout, connectTimeout, outputMapping]");
        expectedException.expect(IllegalArgumentException.class);

        HttpExternalSourceConfig httpExternalSourceConfig = new HttpExternalSourceConfig("localhost", "post", "body", null, null, false, null, null, null);
        httpExternalSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfFieldsOfNestedObjectsAreMissing() {
        expectedException.expectMessage("Missing required fields: [path]");
        expectedException.expect(IllegalArgumentException.class);

        OutputMapping outputMappingWithNullField = new OutputMapping(null);

        outputMappings.put("field", outputMappingWithNullField);

        httpExternalSourceConfig = new HttpExternalSourceConfig("http://localhost",
                "post", "request_body", "4000", "1000", false, "", headerMap, outputMappings);
        httpExternalSourceConfig.validateFields();
    }

    @Test
    public void shouldReturnMandatoryFields() {
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();
        expectedMandatoryFields.put("endpoint", "http://localhost");
        expectedMandatoryFields.put("bodyColumnFromSql", "request_body");
        expectedMandatoryFields.put("streamTimeout", "4000");
        expectedMandatoryFields.put("connectTimeout", "1000");
        expectedMandatoryFields.put("outputMapping", outputMapping);
        HashMap<String, Object> actualMandatoryFields = httpExternalSourceConfig.getMandatoryFields();
        assertEquals(expectedMandatoryFields.get("endpoint"), actualMandatoryFields.get("endpoint"));
        assertEquals(expectedMandatoryFields.get("bodyColumnFromSql"), actualMandatoryFields.get("bodyColumnFromSql"));
        assertEquals(expectedMandatoryFields.get("streamTimeout"), actualMandatoryFields.get("streamTimeout"));
        assertEquals(expectedMandatoryFields.get("connectTimeout"), actualMandatoryFields.get("connectTimeout"));
        assertEquals(outputMapping.getPath(), ((Map<String, OutputMapping>) actualMandatoryFields.get("outputMapping")).get("surge_factor").getPath());
    }
}