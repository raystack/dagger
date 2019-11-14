package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.es.EsSourceConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HttpSourceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    HashMap<String, String> headerMap;
    private HttpSourceConfig httpSourceConfig;
    private HashMap<String, OutputMapping> outputMappings;
    private OutputMapping outputMapping;
    private String streamTimeout;
    private String endpoint;
    private String verb;
    private String requestPattern;
    private String requestVariables;
    private String connectTimeout;
    private boolean failOnErrors;
    private String type;
    private String capacity;

    @Before
    public void setup() {
        headerMap = new HashMap<>();
        headerMap.put("content-type", "application/json");
        outputMappings = new HashMap<>();
        outputMapping = new OutputMapping("$.surge");
        outputMappings.put("surge_factor", outputMapping);
        streamTimeout = "123";
        endpoint = "http://localhost:1234";
        verb = "POST";
        requestPattern = "/customers/customer/%s";
        requestVariables = "customer_id";
        connectTimeout = "234";
        failOnErrors = false;
        type = "com.gojek.esb.booking.BookingLogMessage";
        capacity = "345";
        httpSourceConfig = new HttpSourceConfig(endpoint, verb, requestPattern, requestVariables, streamTimeout, connectTimeout, failOnErrors, type, capacity, headerMap, outputMappings);
    }

    @Test
    public void shouldReturnConnectTimeout() {
        Assert.assertEquals(Integer.parseInt(connectTimeout), (int) httpSourceConfig.getConnectTimeout());
    }

    @Test
    public void shouldReturnEndpoint() {
        Assert.assertEquals(endpoint, httpSourceConfig.getEndpoint());
    }

    @Test
    public void shouldReturnStreamTimeout() {
        Assert.assertEquals(Integer.valueOf(streamTimeout), httpSourceConfig.getStreamTimeout());
    }

    @Test
    public void shouldReturnBodyPattern() {
        Assert.assertEquals(requestPattern, httpSourceConfig.getRequestPattern());
    }

    @Test
    public void shouldReturnBodyVariable() {
        Assert.assertEquals(requestVariables, httpSourceConfig.getRequestVariables());
    }

    @Test
    public void shouldReturnFailOnErrors() {
        Assert.assertEquals(failOnErrors, httpSourceConfig.isFailOnErrors());
    }

    @Test
    public void shouldReturnVerb() {
        Assert.assertEquals(verb, httpSourceConfig.getVerb());
    }

    @Test
    public void shouldReturnType() {
        Assert.assertEquals(type, httpSourceConfig.getType());
    }

    @Test
    public void shouldReturnHeaderMap() {
        Assert.assertEquals(headerMap, httpSourceConfig.getHeaders());
    }

    @Test
    public void shouldReturnOutputMapping() {
        Assert.assertEquals(outputMappings, httpSourceConfig.getOutputMapping());
    }

    @Test
    public void shouldReturnCapacity() {
        Assert.assertEquals(Integer.valueOf(capacity), httpSourceConfig.getCapacity());
    }

    @Test
    public void shouldReturnColumnNames() {
        List<String> actualColumns = httpSourceConfig.getOutputColumns();
        String[] expectedColumns = {"surge_factor"};
        Assert.assertArrayEquals(expectedColumns, actualColumns.toArray());
    }

    @Test
    public void shouldValidate() {
        expectedException = ExpectedException.none();

        httpSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfAllFieldsMissing() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [endpoint, streamTimeout, requestPattern, verb, connectTimeout, outputMapping]");

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig(null, null, null, requestVariables, null, null, false, null, capacity, null, null);
        httpSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfSomeFieldsMissing() {
        expectedException.expectMessage("Missing required fields: [streamTimeout, connectTimeout, outputMapping]");
        expectedException.expect(IllegalArgumentException.class);

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("localhost", "post", "body", requestVariables, null, null, false, null, capacity, null, null);
        httpSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfFieldsOfNestedObjectsAreMissing() {
        expectedException.expectMessage("Missing required fields: [path]");
        expectedException.expect(IllegalArgumentException.class);

        OutputMapping outputMappingWithNullField = new OutputMapping(null);

        outputMappings.put("field", outputMappingWithNullField);

        httpSourceConfig = new HttpSourceConfig("http://localhost",
                "post", "request_body", requestVariables, "4000", "1000", false, "", capacity, headerMap, outputMappings);
        httpSourceConfig.validateFields();
    }

    @Test
    public void shouldReturnMandatoryFields() {
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();
        expectedMandatoryFields.put("endpoint", endpoint);
        expectedMandatoryFields.put("verb", verb);
        expectedMandatoryFields.put("failOnErrors", failOnErrors);
        expectedMandatoryFields.put("capacity", capacity);
        expectedMandatoryFields.put("requestPattern", requestPattern);
        expectedMandatoryFields.put("requestVariables", requestVariables);
        expectedMandatoryFields.put("streamTimeout", streamTimeout);
        expectedMandatoryFields.put("connectTimeout", connectTimeout);
        expectedMandatoryFields.put("outputMapping", outputMapping);
        HashMap<String, Object> actualMandatoryFields = httpSourceConfig.getMandatoryFields();
        assertEquals(expectedMandatoryFields.get("endpoint"), actualMandatoryFields.get("endpoint"));
        assertEquals(expectedMandatoryFields.get("verb"), actualMandatoryFields.get("verb"));
        assertEquals(expectedMandatoryFields.get("failOnErrors"), actualMandatoryFields.get("failOnErrors"));
        assertEquals(expectedMandatoryFields.get("capacity"), actualMandatoryFields.get("capacity"));
        assertEquals(expectedMandatoryFields.get("requestPattern"), actualMandatoryFields.get("requestPattern"));
        assertEquals(expectedMandatoryFields.get("requestVariables"), actualMandatoryFields.get("requestVariables"));
        assertEquals(expectedMandatoryFields.get("streamTimeout"), actualMandatoryFields.get("streamTimeout"));
        assertEquals(expectedMandatoryFields.get("connectTimeout"), actualMandatoryFields.get("connectTimeout"));
        assertEquals(outputMapping.getPath(), ((Map<String, OutputMapping>) actualMandatoryFields.get("outputMapping")).get("surge_factor").getPath());
    }

    @Test
    public void shouldValidateWhenOutputMappingIsEmpty() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [outputMapping]");

        httpSourceConfig = new HttpSourceConfig(endpoint, verb, requestPattern, requestVariables, streamTimeout, connectTimeout, failOnErrors, type, capacity, headerMap, new HashMap<>());

        httpSourceConfig.validateFields();
    }
}