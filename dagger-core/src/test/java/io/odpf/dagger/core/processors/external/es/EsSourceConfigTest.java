package io.odpf.dagger.core.processors.external.es;

import io.odpf.dagger.core.processors.common.OutputMapping;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class EsSourceConfigTest {

    private final String host = "localhost";
    private final String port = "9200";
    private final String user = "test_user";
    private final String password = "mysecretpassword";
    private final String endpointPattern = "/drivers/driver/%s";
    private final String endpointVariables = "driver_id";
    private final String type = "TestProtoMessage";
    private final String capacity = "30";
    private final String connectTimeout = "1000";
    private final String retryTimeout = "2000";
    private final String socketTimeout = "3000";
    private final String streamTimeout = "4000";
    private final boolean failOnErrors = false;
    private final String metricId = "metricId_01";
    private final boolean retainResponseType = false;
    private final Map<String, OutputMapping> outputMappingHashMap =  Collections.unmodifiableMap(new HashMap<String, OutputMapping>() {{
        put("key", new OutputMapping("path"));
    }});

    @Test
    public void getHostShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setHost(host)
                .createEsSourceConfig();
        assertEquals(host, esSourceConfig.getHost());
    }

    @Test
    public void getPortShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setPort(port)
                .createEsSourceConfig();
        assertEquals(Integer.valueOf(port), esSourceConfig.getPort());
    }

    @Test
    public void getUserShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setUser(user).
                createEsSourceConfig();
        assertEquals(user, esSourceConfig.getUser());
    }

    @Test
    public void getPasswordShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setPassword(password)
                .createEsSourceConfig();
        assertEquals(password, esSourceConfig.getPassword());
    }

    @Test
    public void getUserWhenUserIsNullShouldReturnEmptyString() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setUser(null)
                .createEsSourceConfig();
        assertEquals("", esSourceConfig.getUser());
    }

    @Test
    public void getPasswordWhenPasswordIsNullShouldReturnEmptyString() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setPassword(null)
                .createEsSourceConfig();
        assertEquals("", esSourceConfig.getPassword());
    }

    @Test
    public void getEndpointPatternShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setEndpointPattern(endpointPattern)
                .createEsSourceConfig();
        assertEquals(endpointPattern, esSourceConfig.getPattern());
    }

    @Test
    public void getEndpointVariablesShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = getValidEsSourceConfigBuilder()
                .setEndpointVariables(endpointVariables)
                .createEsSourceConfig();
        assertEquals(endpointVariables, esSourceConfig.getVariables());
    }

    @Test
    public void getMetricIdShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setMetricId(metricId)
                .createEsSourceConfig();
        assertEquals(metricId, esSourceConfig.getMetricId());
    }

    @Test
    public void isRetainResponseTypeShouldGetTheRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setRetainResponseType(retainResponseType)
                .createEsSourceConfig();
        assertEquals(retainResponseType, esSourceConfig.isRetainResponseType());
    }

    @Test
    public void isFailOnErrorsShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setFailOnErrors(failOnErrors)
                .createEsSourceConfig();
        assertEquals(failOnErrors, esSourceConfig.isFailOnErrors());
    }


    @Test
    public void getTypeShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setType(type)
                .createEsSourceConfig();
        assertEquals(type, esSourceConfig.getType());
    }

    @Test
    public void hasTypeShouldBeTrueWhenTypeIsPresent() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setType(type)
                .createEsSourceConfig();
        assertTrue(esSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsNull() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setType(null)
                .createEsSourceConfig();
        assertFalse(esSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setType("")
                .createEsSourceConfig();
        assertFalse(esSourceConfig.hasType());
    }

    @Test
    public void getCapacityShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setCapacity(capacity)
                .createEsSourceConfig();
        assertEquals(Integer.valueOf(capacity), esSourceConfig.getCapacity());
    }

    @Test
    public void getRetryTimeoutShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setRetryTimeout(retryTimeout)
                .createEsSourceConfig();
        assertEquals(Integer.valueOf(retryTimeout), esSourceConfig.getRetryTimeout());
    }

    @Test
    public void getSocketTimeoutShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setSocketTimeout(socketTimeout)
                .createEsSourceConfig();
        assertEquals(Integer.valueOf(socketTimeout), esSourceConfig.getSocketTimeout());
    }

    @Test
    public void getStreamTimeoutShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setStreamTimeout(streamTimeout)
                .createEsSourceConfig();
        assertEquals(Integer.valueOf(streamTimeout), esSourceConfig.getStreamTimeout());
    }

    @Test
    public void getConnectTimeoutShouldGetRightConfig() {
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setConnectTimeout(connectTimeout)
                .createEsSourceConfig();
        assertEquals(Integer.valueOf(connectTimeout), esSourceConfig.getConnectTimeout());
    }

    @Test
    public void getOutputColumnNames() {
        HashMap<String, OutputMapping> outputMap = new HashMap<String, OutputMapping>();
        outputMap.put("key", new OutputMapping("path"));
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setOutputMapping(outputMap)
                .createEsSourceConfig();
        assertEquals(Arrays.asList("key"), esSourceConfig.getOutputColumns());
    }

    @Test
    public void shouldReturnPathForOutputField() {
        HashMap<String, OutputMapping> outputMap = new HashMap<>();
        outputMap.put("outputField", new OutputMapping("path"));
        EsSourceConfig esSourceConfig = new EsSourceConfigBuilder()
                .setOutputMapping(outputMap)
                .createEsSourceConfig();
        assertEquals("path", esSourceConfig.getPath("outputField"));
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
        expectedMandatoryFields.put("outputMapping", outputMappingHashMap);
        EsSourceConfig esSourceConfig = getValidEsSourceConfigBuilder().createEsSourceConfig();
        HashMap<String, Object> actualMandatoryFields = esSourceConfig.getMandatoryFields();
        assertEquals(expectedMandatoryFields, actualMandatoryFields);
    }

    @Test
    public void shouldValidateWhenOutputMappingIsEmpty() {

        EsSourceConfigBuilder validEsSourceConfigBuilder = getValidEsSourceConfigBuilder();
        EsSourceConfig esSourceConfig = validEsSourceConfigBuilder
                .setOutputMapping(Collections.emptyMap())
                .createEsSourceConfig();
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> esSourceConfig.validateFields());
        assertEquals("Missing required fields: [outputMapping]", illegalArgumentException.getMessage());
    }

    @Test
    public void shouldValidateWhenEndpointPatternIsEmpty() {
        EsSourceConfigBuilder validEsSourceConfigBuilder = getValidEsSourceConfigBuilder();
        EsSourceConfig esSourceConfig = validEsSourceConfigBuilder
                .setEndpointPattern("")
                .createEsSourceConfig();
        IllegalArgumentException illegalArgumentException =
                assertThrows(IllegalArgumentException.class, () -> esSourceConfig.validateFields());
        assertEquals("Missing required fields: [endpoint_pattern]", illegalArgumentException.getMessage());
    }

    private EsSourceConfigBuilder getValidEsSourceConfigBuilder() {
        return new EsSourceConfigBuilder()
                .setHost(host)
                .setPort(port)
                .setUser(user)
                .setPassword(password)
                .setEndpointPattern(endpointPattern)
                .setEndpointVariables(endpointVariables)
                .setType(type)
                .setCapacity(capacity)
                .setConnectTimeout(connectTimeout)
                .setRetryTimeout(retryTimeout)
                .setSocketTimeout(socketTimeout)
                .setStreamTimeout(streamTimeout)
                .setFailOnErrors(failOnErrors)
                .setOutputMapping(outputMappingHashMap)
                .setMetricId(metricId)
                .setRetainResponseType(retainResponseType);
    }

}
