package io.odpf.dagger.processors.external.http;

import io.odpf.dagger.processors.common.OutputMapping;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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
    private String metricId;
    private boolean retainResponseType;

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
        type = "InputProtoMessage";
        capacity = "345";
        metricId = "metricId-http-01";
        retainResponseType = false;
        httpSourceConfig = new HttpSourceConfig(endpoint, verb, requestPattern, requestVariables, streamTimeout, connectTimeout, failOnErrors, type, capacity, headerMap, outputMappings, metricId, retainResponseType);
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
        Assert.assertEquals(requestPattern, httpSourceConfig.getPattern());
    }

    @Test
    public void shouldReturnBodyVariable() {
        Assert.assertEquals(requestVariables, httpSourceConfig.getRequestVariables());
    }

    @Test
    public void isRetainResponseTypeShouldGetTheRightConfig() {
        assertEquals(retainResponseType, httpSourceConfig.isRetainResponseType());
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
    public void getMetricIdShouldGetRightConfig() {
        assertEquals(metricId, httpSourceConfig.getMetricId());
    }

    @Test
    public void shouldReturnType() {
        Assert.assertEquals(type, httpSourceConfig.getType());
    }

    @Test
    public void hasTypeShouldBeTrueWhenTypeIsPresent() {
        assertTrue(httpSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsNull() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("", "", "", "", null, "", false, null, "", new HashMap<>(), new HashMap<>(), metricId, false);
        assertFalse(httpSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("", "", "", "", "", "", false, "", "", new HashMap<>(), new HashMap<>(), metricId, false);
        assertFalse(httpSourceConfig.hasType());
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

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig(null, null, null, requestVariables, null, null, false, null, capacity, null, null, metricId, retainResponseType);
        httpSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfSomeFieldsMissing() {
        expectedException.expectMessage("Missing required fields: [streamTimeout, connectTimeout, outputMapping]");
        expectedException.expect(IllegalArgumentException.class);

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("localhost", "post", "body", requestVariables, null, null, false, null, capacity, null, null, "metricId_01", retainResponseType);
        httpSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfFieldsOfNestedObjectsAreMissing() {
        expectedException.expectMessage("Missing required fields: [path]");
        expectedException.expect(IllegalArgumentException.class);

        OutputMapping outputMappingWithNullField = new OutputMapping(null);

        outputMappings.put("field", outputMappingWithNullField);

        httpSourceConfig = new HttpSourceConfig("http://localhost",
                "post", "request_body", requestVariables, "4000", "1000", false, "", capacity, headerMap, outputMappings, "metricId_01", retainResponseType);
        httpSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfRequestPatternIsEmpty() {
        expectedException.expectMessage("Missing required fields: [requestPattern]");
        expectedException.expect(IllegalArgumentException.class);

        OutputMapping outputMappingWithNullField = new OutputMapping(null);

        outputMappings.put("field", outputMappingWithNullField);

        httpSourceConfig = new HttpSourceConfig("http://localhost",
                "post", "", requestVariables, "4000", "1000", false, "", capacity, headerMap, outputMappings, "metricId_01", retainResponseType);
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
        expectedMandatoryFields.put("metric_id", metricId);
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

        httpSourceConfig = new HttpSourceConfig(endpoint, verb, requestPattern, requestVariables, streamTimeout, connectTimeout, failOnErrors, type, capacity, headerMap, new HashMap<>(), "metricId_01", retainResponseType);

        httpSourceConfig.validateFields();
    }
}
