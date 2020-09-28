package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class EsSourceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private EsSourceConfig esSourceConfig;
    private String host;
    private String port;
    private String endpointPattern;
    private String endpointVariables;
    private String type;
    private String capacity;
    private String connectTimeout;
    private String retryTimeout;
    private String socketTimeout;
    private String streamTimeout;
    private boolean failOnErrors;
    private HashMap<String, OutputMapping> outputMapping;
    private String metricId;
    private boolean retainResponseType;

    @Before
    public void setUp() {
        host = "localhost";
        port = "9200";
        endpointPattern = "/drivers/driver/%s";
        endpointVariables = "driver_id";
        type = "com.gojek.esb.fraud.DriverProfileFlattenLogMessage";
        capacity = "30";
        connectTimeout = "1000";
        retryTimeout = "2000";
        socketTimeout = "3000";
        streamTimeout = "4000";
        failOnErrors = false;
        outputMapping = new HashMap<>();
        metricId = "metricId_01";
        retainResponseType = false;

        esSourceConfig = new EsSourceConfig(host, port, endpointPattern,
                endpointVariables, type, capacity,
                connectTimeout, retryTimeout, socketTimeout, streamTimeout, failOnErrors, outputMapping, metricId, retainResponseType);

    }

    @Test
    public void getHostShouldGetRightConfig() {
        assertEquals(host, esSourceConfig.getHost());
    }

    @Test
    public void getPortShouldGetRightConfig() {
        assertEquals(Integer.valueOf(port), esSourceConfig.getPort());
    }

    @Test
    public void getEndpointPatternShouldGetRightConfig() {
        assertEquals(endpointPattern, esSourceConfig.getPattern());
    }

    @Test
    public void getEndpointVariablesShouldGetRightConfig() {
        assertEquals(endpointVariables, esSourceConfig.getVariables());
    }

    @Test
    public void getMetricIdShouldGetRightConfig() {
        assertEquals(metricId, esSourceConfig.getMetricId());
    }

    @Test
    public void isRetainResponseTypeShouldGetTheRightConfig() {
        assertEquals(retainResponseType, esSourceConfig.isRetainResponseType());
    }

    @Test
    public void isFailOnErrorsShouldGetRightConfig() {
        assertEquals(failOnErrors, esSourceConfig.isFailOnErrors());
    }


    @Test
    public void getTypeShouldGetRightConfig() {
        assertEquals(type, esSourceConfig.getType());
    }

    @Test
    public void hasTypeShouldBeTrueWhenTypeIsPresent() {
        assertEquals(true, esSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsNull() {
        EsSourceConfig esSourceConfig = new EsSourceConfig("", "", "", "", null, "", "", "", "", "", false, new HashMap<>(), metricId, false);
        assertEquals(false, esSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        EsSourceConfig esSourceConfig = new EsSourceConfig("", "", "", "", "", "", "", "", "", "", false, new HashMap<>(), metricId, false);
        assertEquals(false, esSourceConfig.hasType());
    }

    @Test
    public void getCapacityShouldGetRightConfig() {
        assertEquals(Integer.valueOf(capacity), esSourceConfig.getCapacity());
    }

    @Test
    public void getRetryTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(retryTimeout), esSourceConfig.getRetryTimeout());
    }

    @Test
    public void getSocketTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(socketTimeout), esSourceConfig.getSocketTimeout());
    }

    @Test
    public void getStreamTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(streamTimeout), esSourceConfig.getStreamTimeout());
    }

    @Test
    public void getConnectTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(connectTimeout), esSourceConfig.getConnectTimeout());
    }

    @Test
    public void getOutputColumnNames() {
        List<String> keys = new ArrayList<>();
        keys.add("key");
        outputMapping.put("key", new OutputMapping("path"));
        assertEquals(keys, esSourceConfig.getOutputColumns());
    }

    @Test
    public void shouldReturnPathForOutputField() {
        outputMapping.put("outputField", new OutputMapping("path"));
        Assert.assertEquals("path", esSourceConfig.getPath("outputField"));
    }

    @Test
    public void shouldReturnMandatoryFields() {
        HashMap<String, Object> expectedMandatoryFields = new HashMap<>();
        expectedMandatoryFields.put("host", host);
        expectedMandatoryFields.put("port", port);
        expectedMandatoryFields.put("endpoint_pattern", endpointPattern);
        expectedMandatoryFields.put("capacity", capacity);
        expectedMandatoryFields.put("connect_timeout", connectTimeout);
        expectedMandatoryFields.put("retry_timeout", retryTimeout);
        expectedMandatoryFields.put("socket_timeout", socketTimeout);
        expectedMandatoryFields.put("stream_timeout", streamTimeout);
        expectedMandatoryFields.put("fail_on_errors", failOnErrors);
        expectedMandatoryFields.put("outputMapping", outputMapping);
        HashMap<String, Object> actualMandatoryFields = esSourceConfig.getMandatoryFields();
        assertArrayEquals(expectedMandatoryFields.values().toArray(), actualMandatoryFields.values().toArray());
    }

    @Test
    public void shouldValidateWhenOutputMappingIsEmpty() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [outputMapping]");

        EsSourceConfig esSourceConfig = new EsSourceConfig(host, port, endpointPattern, endpointVariables, type, capacity, connectTimeout, retryTimeout, socketTimeout, streamTimeout, false, new HashMap<>(), metricId, retainResponseType);

        esSourceConfig.validateFields();
    }

}
