package com.gotocompany.dagger.core.processors.external.http;

import com.gotocompany.dagger.core.processors.common.OutputMapping;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static org.junit.Assert.*;

public class HttpSourceConfigTest {

    private HashMap<String, String> headerMap;
    private HttpSourceConfig defaultHttpSourceConfig;
    private HashMap<String, OutputMapping> outputMappings;
    private OutputMapping outputMapping;
    private String streamTimeout;
    private String endpoint;
    private String endpointVariable;
    private String verb;
    private String requestPattern;
    private String requestVariables;
    private String headerPattern;
    private String headerVariables;
    private String connectTimeout;
    private boolean failOnErrors;
    private String failOnErrorsCodeRange;
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
        endpointVariable = "";
        verb = "POST";
        requestPattern = "/customers/customer/%s";
        requestVariables = "customer_id";
        headerPattern = "{\"X_KEY\":\"%s\"}";
        headerVariables = "customer_id";
        connectTimeout = "234";
        failOnErrors = false;
        failOnErrorsCodeRange = "";
        type = "InputProtoMessage";
        capacity = "345";
        metricId = "metricId-http-01";
        retainResponseType = false;
        defaultHttpSourceConfig = new HttpSourceConfig(endpoint, endpointVariable, verb, requestPattern, requestVariables, headerPattern, headerVariables, streamTimeout, connectTimeout, failOnErrors, failOnErrorsCodeRange, type, capacity, headerMap, outputMappings, metricId, retainResponseType);
    }

    @Test
    public void shouldReturnConnectTimeout() {
        assertEquals(parseInt(connectTimeout), (int) defaultHttpSourceConfig.getConnectTimeout());
    }

    @Test
    public void shouldReturnEndpoint() {
        assertEquals(endpoint, defaultHttpSourceConfig.getEndpoint());
    }

    @Test
    public void shouldReturnStreamTimeout() {
        assertEquals(Integer.valueOf(streamTimeout), defaultHttpSourceConfig.getStreamTimeout());
    }

    @Test
    public void shouldReturnBodyPattern() {
        assertEquals(requestPattern, defaultHttpSourceConfig.getPattern());
    }

    @Test
    public void shouldReturnBodyVariable() {
        assertEquals(requestVariables, defaultHttpSourceConfig.getRequestVariables());
    }

    @Test
    public void isRetainResponseTypeShouldGetTheRightConfig() {
        assertEquals(retainResponseType, defaultHttpSourceConfig.isRetainResponseType());
    }

    @Test
    public void shouldReturnFailOnErrors() {
        assertEquals(failOnErrors, defaultHttpSourceConfig.isFailOnErrors());
    }

    @Test
    public void shouldReturnVerb() {
        assertEquals(verb, defaultHttpSourceConfig.getVerb());
    }

    @Test
    public void getMetricIdShouldGetRightConfig() {
        assertEquals(metricId, defaultHttpSourceConfig.getMetricId());
    }

    @Test
    public void shouldReturnType() {
        assertEquals(type, defaultHttpSourceConfig.getType());
    }

    @Test
    public void hasTypeShouldBeTrueWhenTypeIsPresent() {
        assertTrue(defaultHttpSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsNull() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("", "", "", "", "", "", "", null, "", false, null, null, "", new HashMap<>(), new HashMap<>(), metricId, false);
        assertFalse(httpSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("", "", "", "", "", "", "", "", "", false, null, "", "", new HashMap<>(), new HashMap<>(), metricId, false);
        assertFalse(httpSourceConfig.hasType());
    }

    @Test
    public void shouldReturnHeaderMap() {
        assertEquals(headerMap, defaultHttpSourceConfig.getHeaders());
    }

    @Test
    public void shouldReturnOutputMapping() {
        assertEquals(outputMappings, defaultHttpSourceConfig.getOutputMapping());
    }

    @Test
    public void shouldReturnCapacity() {
        assertEquals(Integer.valueOf(capacity), defaultHttpSourceConfig.getCapacity());
    }

    @Test
    public void shouldReturnColumnNames() {
        List<String> actualColumns = defaultHttpSourceConfig.getOutputColumns();
        String[] expectedColumns = {"surge_factor"};
        assertArrayEquals(expectedColumns, actualColumns.toArray());
    }

    @Test
    public void shouldValidate() {
        defaultHttpSourceConfig.validateFields();
    }

    @Test
    public void shouldThrowExceptionIfAllFieldsMissing() {

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig(null, null, null, null, requestVariables, null, null, null, null, false, null, null, capacity, null, null, metricId, retainResponseType);
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> httpSourceConfig.validateFields());
        assertEquals("Missing required fields: [endpoint, streamTimeout, requestPattern, verb, connectTimeout, outputMapping]", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfSomeFieldsMissing() {

        HttpSourceConfig httpSourceConfig = new HttpSourceConfig("localhost", "", "post", "body", requestVariables, null, null, null, null, false, null, null, capacity, null, null, "metricId_01", retainResponseType);
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> httpSourceConfig.validateFields());
        assertEquals("Missing required fields: [streamTimeout, connectTimeout, outputMapping]", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfFieldsOfNestedObjectsAreMissing() {

        OutputMapping outputMappingWithNullField = new OutputMapping(null);

        outputMappings.put("field", outputMappingWithNullField);

        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost", "",
                "post", "request_body", requestVariables, "", "", "4000", "1000", false, null, "", capacity, headerMap, outputMappings, "metricId_01", retainResponseType);
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> defaultHttpSourceConfig.validateFields());
        assertEquals("Missing required fields: [path]", exception.getMessage());

    }

    @Test
    public void shouldThrowExceptionIfRequestPatternIsEmpty() {

        OutputMapping outputMappingWithNullField = new OutputMapping(null);

        outputMappings.put("field", outputMappingWithNullField);

        defaultHttpSourceConfig = new HttpSourceConfig("http://localhost", "",
                "post", "", requestVariables, "", "", "4000", "1000", false, null, "", capacity, headerMap, outputMappings, "metricId_01", retainResponseType);
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> defaultHttpSourceConfig.validateFields());
        assertEquals("Missing required fields: [requestPattern]", exception.getMessage());

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
        HashMap<String, Object> actualMandatoryFields = defaultHttpSourceConfig.getMandatoryFields();
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

        defaultHttpSourceConfig = new HttpSourceConfig(endpoint, endpointVariable, verb, requestPattern, requestVariables, headerPattern, headerVariables, streamTimeout, connectTimeout, failOnErrors, failOnErrorsCodeRange, type, capacity, headerMap, new HashMap<>(), "metricId_01", retainResponseType);

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> defaultHttpSourceConfig.validateFields());
        assertEquals("Missing required fields: [outputMapping]", exception.getMessage());
    }
}
