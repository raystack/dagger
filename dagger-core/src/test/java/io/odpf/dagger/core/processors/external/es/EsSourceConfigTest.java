package io.odpf.dagger.core.processors.external.es;

import io.odpf.dagger.core.processors.common.OutputMapping;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EsSourceConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private EsSourceConfig defaultEsSourceConfig;
    private String host;
    private String port;
    private String user;
    private String password;
    private String endpointPattern;
    private String endpointVariables;
    private String type;
    private String capacity;
    private String connectTimeout;
    private String retryTimeout;
    private String socketTimeout;
    private String streamTimeout;
    private boolean failOnErrors;
    private HashMap<String, OutputMapping> defaultOutputMapping;
    private String metricId;
    private boolean retainResponseType;

    @Before
    public void setUp() {
        host = "localhost";
        port = "9200";
        user = "test_user";
        password = "mysecretpassword";
        endpointPattern = "/drivers/driver/%s";
        endpointVariables = "driver_id";
        type = "TestProtoMessage";
        capacity = "30";
        connectTimeout = "1000";
        retryTimeout = "2000";
        socketTimeout = "3000";
        streamTimeout = "4000";
        failOnErrors = false;
        defaultOutputMapping = new HashMap<>();
        metricId = "metricId_01";
        retainResponseType = false;

        defaultEsSourceConfig = new EsSourceConfig(host, port, user, password, endpointPattern,
                endpointVariables, type, capacity,
                connectTimeout, retryTimeout, socketTimeout, streamTimeout, failOnErrors, defaultOutputMapping, metricId, retainResponseType);
    }

    @Test
    public void getHostShouldGetRightConfig() {
        assertEquals(host, defaultEsSourceConfig.getHost());
    }

    @Test
    public void getPortShouldGetRightConfig() {
        assertEquals(Integer.valueOf(port), defaultEsSourceConfig.getPort());
    }

    @Test
    public void getUserShouldGetRightConfig() {
        assertEquals(user, defaultEsSourceConfig.getUser());
    }

    @Test
    public void getPasswordShouldGetRightConfig() {
        assertEquals(password, defaultEsSourceConfig.getPassword());
    }

    @Test
    public void getUserWhenUserIsNullShouldReturnEmptyString() {
        EsSourceConfig esSourceConfig = new EsSourceConfig(host, port, null, password, endpointPattern,
                endpointVariables, type, capacity,
                connectTimeout, retryTimeout, socketTimeout, streamTimeout, failOnErrors, defaultOutputMapping, metricId, retainResponseType);
        assertEquals("", esSourceConfig.getUser());
    }

    @Test
    public void getPasswordWhenPasswordIsNullShouldReturnEmptyString() {
        EsSourceConfig esSourceConfig = new EsSourceConfig(host, port, user, null, endpointPattern,
                endpointVariables, type, capacity,
                connectTimeout, retryTimeout, socketTimeout, streamTimeout, failOnErrors, defaultOutputMapping, metricId, retainResponseType);
        assertEquals("", esSourceConfig.getPassword());
    }

    @Test
    public void getEndpointPatternShouldGetRightConfig() {
        assertEquals(endpointPattern, defaultEsSourceConfig.getPattern());
    }

    @Test
    public void getEndpointVariablesShouldGetRightConfig() {
        assertEquals(endpointVariables, defaultEsSourceConfig.getVariables());
    }

    @Test
    public void getMetricIdShouldGetRightConfig() {
        assertEquals(metricId, defaultEsSourceConfig.getMetricId());
    }

    @Test
    public void isRetainResponseTypeShouldGetTheRightConfig() {
        assertEquals(retainResponseType, defaultEsSourceConfig.isRetainResponseType());
    }

    @Test
    public void isFailOnErrorsShouldGetRightConfig() {
        assertEquals(failOnErrors, defaultEsSourceConfig.isFailOnErrors());
    }


    @Test
    public void getTypeShouldGetRightConfig() {
        assertEquals(type, defaultEsSourceConfig.getType());
    }

    @Test
    public void hasTypeShouldBeTrueWhenTypeIsPresent() {
        assertTrue(defaultEsSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsNull() {
        EsSourceConfig esSourceConfig = new EsSourceConfig("", "", "", "", "", "", null, "", "", "", "", "", false, new HashMap<>(), metricId, false);
        assertFalse(esSourceConfig.hasType());
    }

    @Test
    public void hasTypeShouldBeFalseWhenTypeIsEmpty() {
        EsSourceConfig esSourceConfig = new EsSourceConfig("", "", "", "", "", "", "", "", "", "", "", "", false, new HashMap<>(), metricId, false);
        assertFalse(esSourceConfig.hasType());
    }

    @Test
    public void getCapacityShouldGetRightConfig() {
        assertEquals(Integer.valueOf(capacity), defaultEsSourceConfig.getCapacity());
    }

    @Test
    public void getRetryTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(retryTimeout), defaultEsSourceConfig.getRetryTimeout());
    }

    @Test
    public void getSocketTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(socketTimeout), defaultEsSourceConfig.getSocketTimeout());
    }

    @Test
    public void getStreamTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(streamTimeout), defaultEsSourceConfig.getStreamTimeout());
    }

    @Test
    public void getConnectTimeoutShouldGetRightConfig() {
        assertEquals(Integer.valueOf(connectTimeout), defaultEsSourceConfig.getConnectTimeout());
    }

    @Test
    public void getOutputColumnNames() {
        List<String> keys = new ArrayList<>();
        keys.add("key");
        defaultOutputMapping.put("key", new OutputMapping("path"));
        assertEquals(keys, defaultEsSourceConfig.getOutputColumns());
    }

    @Test
    public void shouldReturnPathForOutputField() {
        defaultOutputMapping.put("outputField", new OutputMapping("path"));
        assertEquals("path", defaultEsSourceConfig.getPath("outputField"));
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
        expectedMandatoryFields.put("outputMapping", defaultOutputMapping);
        HashMap<String, Object> actualMandatoryFields = defaultEsSourceConfig.getMandatoryFields();
        assertArrayEquals(expectedMandatoryFields.values().toArray(), actualMandatoryFields.values().toArray());
    }

    @Test
    public void shouldValidateWhenOutputMappingIsEmpty() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [outputMapping]");

        EsSourceConfig esSourceConfig = new EsSourceConfig(host, port, user, password, endpointPattern, endpointVariables, type, capacity, connectTimeout, retryTimeout, socketTimeout, streamTimeout, false, new HashMap<>(), metricId, retainResponseType);

        esSourceConfig.validateFields();
    }

    @Test
    public void shouldValidateWhenEndpointPatternIsEmpty() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Missing required fields: [endpoint_pattern]");
        Map<String, OutputMapping> outputMapping = new HashMap<>();
        outputMapping.put("test", new OutputMapping("test"));
        EsSourceConfig esSourceConfig = new EsSourceConfig(host, port, user, password, "", endpointVariables, type, capacity, connectTimeout, retryTimeout, socketTimeout, streamTimeout, false, outputMapping, metricId, retainResponseType);

        esSourceConfig.validateFields();
    }
}
